package engine

import (
	"encoding/json"
	"fmt"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Restore replacement protocol.
//
// A data directory holds at most one restore.intent marker. Its presence
// means a restore installation was committed by the maintainer but has not
// finished: normal startup refuses the directory, while the install and
// abort entry points use the marker to recognize old state, the selected
// image and the interrupted phase. The marker is written before any live
// file moves and removed only after the new checkpoint and journal are
// verified, so an interrupted install can neither look like a finished new
// database nor glue the old journal suffix onto another image.
//
// Space boundary: the staged incoming image is the only extra payload (one
// image size); old state is quarantined by rename and discarded only after
// the installed state verifies, which keeps a trustworthy rollback path.

const (
	intentFile         = "restore.intent"
	incomingFile       = "restore.incoming"
	replacedCheckpoint = "replaced.checkpoint.lmi"
	replacedJournal    = "replaced.journal.lmw"
)

type replaceIntent struct {
	Format        int      `json:"format"`
	Replace       bool     `json:"replace"`
	Image         string   `json:"image_sha256"`
	Sequence      uint64   `json:"sequence"`
	HadCheckpoint bool     `json:"had_checkpoint"`
	Quarantined   []string `json:"quarantined"`
}

type dirState struct {
	hasIntent             bool
	intent                replaceIntent
	intentBroken          bool
	hasIncoming           bool
	hasReplacedCheckpoint bool
	hasReplacedJournal    bool
	hasLockFile           bool
	hasTempResidue        bool
	foreign               []string
	hasCheckpoint         bool
	checkpointValid       bool
	checkpointDigest      string
	journal               journal.Info
}

func knownEntry(name string) bool {
	switch name {
	case "checkpoint.lmi", "journal.lmw", "lamina.lock", intentFile, incomingFile, replacedCheckpoint, replacedJournal:
		return true
	}
	for _, prefix := range []string{".lamina-image-", ".lamina-journal-", ".lamina-restore-"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// inspectDir classifies the directory contents shared by install, abort and
// startup recognition.
func inspectDir(dir string) (dirState, error) {
	var st dirState
	entries, err := os.ReadDir(dir)
	if err != nil {
		return st, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !knownEntry(name) {
			st.foreign = append(st.foreign, name)
			continue
		}
		switch name {
		case intentFile:
			st.hasIntent = true
			b, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil || json.Unmarshal(b, &st.intent) != nil || st.intent.Format != 1 {
				st.intentBroken = true
			}
		case incomingFile:
			st.hasIncoming = true
		case replacedCheckpoint:
			st.hasReplacedCheckpoint = true
		case replacedJournal:
			st.hasReplacedJournal = true
		case "lamina.lock":
			st.hasLockFile = true
		case "checkpoint.lmi":
			st.hasCheckpoint = true
			if _, digest, err := snapshot.ReadDigest(filepath.Join(dir, name)); err == nil {
				st.checkpointValid = true
				st.checkpointDigest = digest
			}
		default:
			st.hasTempResidue = true
		}
	}
	st.journal, err = journal.Inspect(dir)
	if err != nil {
		return st, err
	}
	return st, nil
}

// checkReplaceIntent is the normal-startup half of the shared recognition:
// a directory with an unresolved install is never opened as a database.
func checkReplaceIntent(dir string) error {
	b, err := os.ReadFile(filepath.Join(dir, intentFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var intent replaceIntent
	if json.Unmarshal(b, &intent) != nil || intent.Format != 1 {
		return fmt.Errorf("data directory holds an unfinished restore install with an unreadable %s; inspect the directory manually", intentFile)
	}
	kind := "restore"
	if intent.Replace {
		kind = "restore replacement"
	}
	return fmt.Errorf("data directory holds an unfinished %s of image sequence %d; resume with -restore (and -replace) or roll back with -replace-abort", kind, intent.Sequence)
}

func lockDir(dir string) (*os.File, error) {
	lock, err := os.OpenFile(filepath.Join(dir, "lamina.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data directory already open: %w", err)
	}
	return lock, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func writeAtomic(dir, name string, data []byte) error {
	f, err := os.CreateTemp(dir, ".lamina-restore-")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(temp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

// installBreakpoint is a deterministic crash hook for the production
// walkthrough: LAMINA_INSTALL_BREAK=<phase> stops the install at a phase
// boundary exactly as a crash would, leaving the intent marker behind.
func installBreakpoint(phase string) {
	if os.Getenv("LAMINA_INSTALL_BREAK") == phase {
		os.Exit(2)
	}
}

// Restore installs a validated backup into a new directory, preserving the
// original contract: an existing directory is rejected unless it only holds
// an interrupted restore of the same image, which is resumed.
func Restore(dir string, data []byte) error {
	image, digest, err := snapshot.DecodeDigest(data)
	if err != nil {
		return err
	}
	fresh := true
	if err := os.Mkdir(dir, 0700); err != nil {
		if !os.IsExist(err) {
			return err
		}
		fresh = false
	}
	installed := false
	if fresh {
		defer func() {
			if !installed {
				for _, name := range []string{"checkpoint.lmi", "journal.lmw", "lamina.lock", intentFile, incomingFile} {
					os.Remove(filepath.Join(dir, name))
				}
				os.Remove(dir)
			}
		}()
	}
	lock, err := lockDir(dir)
	if err != nil {
		if fresh {
			return err
		}
		return fmt.Errorf("restore requires a new directory: %w", err)
	}
	defer lock.Close()
	st, err := inspectDir(dir)
	if err != nil {
		return err
	}
	resume := false
	if !fresh {
		switch {
		case st.hasIntent && !st.intentBroken && st.intent.Replace:
			return fmt.Errorf("directory holds an unfinished replacement; resume it with -restore and -replace or roll back with -replace-abort")
		case st.hasIntent && !st.intentBroken && st.intent.Image == digest && len(st.foreign) == 0:
			resume = true
		case st.hasIntent:
			return fmt.Errorf("directory holds an unfinished restore of a different image; remove it or restore the same backup")
		case residueOnly(st):
			slog.Info("discarding residue of an interrupted restore", "dir", dir)
		default:
			return fmt.Errorf("restore requires a new directory: %w", os.ErrExist)
		}
	}
	if err := installImage(dir, image, digest, data, false, resume); err != nil {
		return err
	}
	installed = true
	return nil
}

// residueOnly reports whether the directory holds nothing but staging
// residue from an interrupted restore that never reached its intent marker.
func residueOnly(st dirState) bool {
	if st.hasIntent || st.hasCheckpoint || st.journal.Exists || len(st.foreign) > 0 || st.hasReplacedCheckpoint || st.hasReplacedJournal {
		return false
	}
	return st.hasIncoming || st.hasLockFile || st.hasTempResidue
}

// RestoreReplace installs a validated backup into an existing offline data
// directory owned by this database, explicitly reverting it to the selected
// image. Commits beyond the image are quarantined, never mixed in.
func RestoreReplace(dir string, data []byte) error {
	image, digest, err := snapshot.DecodeDigest(data)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("-replace requires an existing lamina data directory; use -restore without -replace for a new directory")
	}
	lock, err := lockDir(dir)
	if err != nil {
		return err
	}
	defer lock.Close()
	st, err := inspectDir(dir)
	if err != nil {
		return err
	}
	if len(st.foreign) > 0 {
		return fmt.Errorf("data directory contains unrelated files: %s", strings.Join(st.foreign, ", "))
	}
	if st.hasIntent {
		if st.intentBroken {
			return fmt.Errorf("unfinished replacement has an unreadable %s; inspect the directory manually", intentFile)
		}
		if !st.intent.Replace {
			return fmt.Errorf("directory holds an unfinished fresh restore, not a replacement; resume it with -restore or remove the directory")
		}
		if st.intent.Image != digest {
			return fmt.Errorf("directory holds an unfinished replacement of a different image (sequence %d); resume with the same backup or roll back with -replace-abort", st.intent.Sequence)
		}
		slog.Info("resuming interrupted restore replacement", "dir", dir, "sequence", image.Sequence)
		return installImage(dir, image, digest, data, true, true)
	}
	if st.hasReplacedCheckpoint || st.hasReplacedJournal {
		return fmt.Errorf("directory holds quarantined files without an intent marker; inspect the directory manually")
	}
	if !st.journal.Exists {
		return fmt.Errorf("not a lamina data directory: journal.lmw missing")
	}
	if !st.journal.Valid {
		return fmt.Errorf("journal header invalid; cannot confirm the directory belongs to lamina")
	}
	if st.checkpointValid && st.checkpointDigest == digest && st.journal.Empty && st.journal.Base == image.Sequence {
		slog.Info("selected image already installed", "dir", dir, "sequence", image.Sequence)
		return nil
	}
	slog.Info("replacing data directory with selected image", "dir", dir, "sequence", image.Sequence)
	return installImage(dir, image, digest, data, true, false)
}

// installImage runs the ordered, idempotent install phases. Every phase is
// safe to re-run, so an interrupted install resumes by calling it again.
func installImage(dir string, image snapshot.Image, digest string, data []byte, replace bool, resume bool) error {
	st, err := inspectDir(dir)
	if err != nil {
		return err
	}
	// Phase 1: stage the incoming image next to the old state.
	staged := st.checkpointValid && st.checkpointDigest == digest
	if !staged {
		if err := writeAtomic(dir, incomingFile, data); err != nil {
			return err
		}
	}
	installBreakpoint("staged")
	// Phase 2: durable intent; from here on normal startup refuses the directory.
	// The quarantine plan is recorded up front so a later rollback can tell
	// exactly where every piece of the old state must be.
	intent := replaceIntent{Format: 1, Replace: replace, Image: digest, Sequence: image.Sequence, HadCheckpoint: st.hasCheckpoint, Quarantined: []string{}}
	if replace {
		if st.hasCheckpoint && !(st.checkpointValid && st.checkpointDigest == digest) {
			intent.Quarantined = append(intent.Quarantined, "checkpoint")
		}
		if st.journal.Exists && !(st.journal.Valid && st.journal.Empty && st.journal.Base == image.Sequence) {
			intent.Quarantined = append(intent.Quarantined, "journal")
		}
	}
	if resume && st.hasIntent && !st.intentBroken {
		intent = st.intent
	}
	encoded, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	if err := writeAtomic(dir, intentFile, encoded); err != nil {
		return err
	}
	installBreakpoint("intent")
	// Phase 3: quarantine the old live state by rename; it stays reachable
	// for rollback until the new state verifies.
	if replace {
		if st.hasCheckpoint && !(st.checkpointValid && st.checkpointDigest == digest) && !st.hasReplacedCheckpoint {
			if err := os.Rename(filepath.Join(dir, "checkpoint.lmi"), filepath.Join(dir, replacedCheckpoint)); err != nil {
				return err
			}
		}
		keepJournal := st.journal.Exists && st.journal.Valid && st.journal.Empty && st.journal.Base == image.Sequence
		if st.journal.Exists && !keepJournal && !st.hasReplacedJournal {
			if err := os.Rename(filepath.Join(dir, "journal.lmw"), filepath.Join(dir, replacedJournal)); err != nil {
				return err
			}
		}
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	installBreakpoint("quarantined")
	// Phase 4: install the selected image and a fresh journal at its base.
	if !staged {
		if err := os.Rename(filepath.Join(dir, incomingFile), filepath.Join(dir, "checkpoint.lmi")); err != nil {
			return err
		}
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	journalInfo, err := journal.Inspect(dir)
	if err != nil {
		return err
	}
	if !journalInfo.Exists {
		if err := journal.Create(dir, image.Sequence); err != nil {
			return err
		}
	}
	installBreakpoint("installed")
	// Phase 5: verify the installed state before discarding anything.
	if _, installedDigest, err := snapshot.ReadDigest(filepath.Join(dir, "checkpoint.lmi")); err != nil || installedDigest != digest {
		return fmt.Errorf("installed checkpoint failed verification")
	}
	journalInfo, err = journal.Inspect(dir)
	if err != nil {
		return err
	}
	if !journalInfo.Valid || journalInfo.Base != image.Sequence || !journalInfo.Empty {
		return fmt.Errorf("installed journal failed verification")
	}
	// Phase 6: cleanup boundary; the intent marker is removed last.
	for _, name := range []string{replacedCheckpoint, replacedJournal, incomingFile} {
		os.Remove(filepath.Join(dir, name))
	}
	if err := os.Remove(filepath.Join(dir, intentFile)); err != nil {
		return err
	}
	return syncDir(dir)
}

// AbortReplace rolls an interrupted replacement back to the old state. It is
// possible in every phase before cleanup discarded the quarantined files.
func AbortReplace(dir string) error {
	lock, err := lockDir(dir)
	if err != nil {
		return err
	}
	defer lock.Close()
	st, err := inspectDir(dir)
	if err != nil {
		return err
	}
	if !st.hasIntent {
		return fmt.Errorf("no interrupted replacement in %s", dir)
	}
	if st.intentBroken {
		return fmt.Errorf("unfinished replacement has an unreadable %s; inspect the directory manually", intentFile)
	}
	if !st.intent.Replace {
		return fmt.Errorf("directory holds an unfinished fresh restore; remove the directory or resume it with -restore")
	}
	intent := st.intent
	quarantinedCheckpoint, quarantinedJournal := false, false
	for _, name := range intent.Quarantined {
		quarantinedCheckpoint = quarantinedCheckpoint || name == "checkpoint"
		quarantinedJournal = quarantinedJournal || name == "journal"
	}
	newCheckpointLive := st.hasCheckpoint && st.checkpointValid && st.checkpointDigest == intent.Image
	newJournalLive := st.journal.Exists && st.journal.Valid && st.journal.Empty && st.journal.Base == intent.Sequence
	// Every quarantined piece of the old state must still be present, and
	// every piece never quarantined must still be live.
	if quarantinedCheckpoint && !st.hasReplacedCheckpoint || quarantinedJournal && !st.hasReplacedJournal {
		return fmt.Errorf("old state was already partially discarded; cannot roll back — resume the replacement with -restore and -replace")
	}
	if !quarantinedCheckpoint && intent.HadCheckpoint && !st.hasCheckpoint {
		return fmt.Errorf("old checkpoint missing; cannot roll back — resume the replacement with -restore and -replace")
	}
	if !quarantinedJournal && !st.journal.Exists {
		return fmt.Errorf("old journal missing; cannot roll back — resume the replacement with -restore and -replace")
	}
	if quarantinedCheckpoint && st.hasCheckpoint && !newCheckpointLive {
		return fmt.Errorf("inconsistent state: two old checkpoints; inspect the directory manually")
	}
	if quarantinedJournal && st.journal.Exists && !newJournalLive {
		return fmt.Errorf("inconsistent state: two old journals; inspect the directory manually")
	}
	// Remove the installed new state, then bring quarantined files back.
	if st.hasCheckpoint && (quarantinedCheckpoint || !intent.HadCheckpoint) {
		if !newCheckpointLive {
			return fmt.Errorf("live checkpoint is not the selected image; inspect the directory manually")
		}
		if err := os.Remove(filepath.Join(dir, "checkpoint.lmi")); err != nil {
			return err
		}
	}
	if quarantinedJournal && st.journal.Exists {
		if err := os.Remove(filepath.Join(dir, "journal.lmw")); err != nil {
			return err
		}
	}
	if st.hasReplacedCheckpoint {
		if err := os.Rename(filepath.Join(dir, replacedCheckpoint), filepath.Join(dir, "checkpoint.lmi")); err != nil {
			return err
		}
	}
	if st.hasReplacedJournal {
		if err := os.Rename(filepath.Join(dir, replacedJournal), filepath.Join(dir, "journal.lmw")); err != nil {
			return err
		}
	}
	os.Remove(filepath.Join(dir, incomingFile))
	if err := syncDir(dir); err != nil {
		return err
	}
	// Verify the rolled-back state before dropping the intent marker.
	journalInfo, err := journal.Inspect(dir)
	if err != nil || !journalInfo.Exists || !journalInfo.Valid {
		return fmt.Errorf("rolled back journal failed verification")
	}
	if intent.HadCheckpoint {
		if _, err := os.Stat(filepath.Join(dir, "checkpoint.lmi")); err != nil {
			return fmt.Errorf("rolled back checkpoint missing")
		}
	}
	if err := os.Remove(filepath.Join(dir, intentFile)); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	slog.Info("restore replacement rolled back", "dir", dir)
	return nil
}
