package bitcask

import "testing"

func BenchmarkMergeManifestEmptyTargetRecovery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		opt := DefaultOptions
		opt.Dir = b.TempDir()
		if err := writeEntryToFile(opt, 0, newTestEntry("bench", []byte("obsolete"), []byte("value"), 1, Committed)); err != nil {
			b.Fatal(err)
		}
		dummy := &DB{opt: opt}
		if err := dummy.writeMergeManifest(mergeManifest{
			Phase:             mergeManifestInstalled,
			SourceFileID:      0,
			FirstTargetFileID: 1,
			LastTargetFileID:  0,
		}); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := recoverMergeManifest(opt); err != nil {
			b.Fatal(err)
		}
	}
}

func writeEntryToFile(opt Options, fileID int64, entry *Entry) error {
	file, err := NewDataFile(opt.Dir, fileID, opt.SegmentSize, opt.RWMode)
	if err != nil {
		return err
	}
	if _, err := file.WriteAt(entry.Encode(), 0); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
