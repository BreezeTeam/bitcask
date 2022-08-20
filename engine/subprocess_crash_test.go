package bitcask

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	faultmodel "github.com/BreezeTeam/bitcask/experiments/fault"
)

const (
	crashChildEnv              = "BITCASK_CRASH_CHILD"
	crashDirEnv                = "BITCASK_CRASH_DIR"
	crashScenarioEnv           = "BITCASK_CRASH_SCENARIO"
	crashMetadataStageEnv      = "BITCASK_CRASH_METADATA_STAGE"
	crashMetadataOccurrenceEnv = "BITCASK_CRASH_METADATA_OCCURRENCE"

	// groupSubprocessWriters is the number of concurrent group-commit writers
	// the group-epoch crash child launches before abrupt termination.
	groupSubprocessWriters = 8
)

func TestCrashExplorerSubprocessChild(t *testing.T) {
	if os.Getenv(crashChildEnv) != "1" {
		return
	}
	opt := DefaultOptions
	opt.Dir = os.Getenv(crashDirEnv)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	scenario := os.Getenv(crashScenarioEnv)
	switch scenario {
	case "committed-before-exit":
		db, err := Open(opt)
		if err != nil {
			os.Exit(21)
		}
		if err := db.Update(func(tx *Tx) error {
			return tx.Put("crash", []byte("committed"), []byte("value"), Persistent)
		}); err != nil {
			os.Exit(22)
		}
		os.Exit(97)
	case "before-commit-marker":
		opt.FaultInjection.Enable = true
		opt.FaultInjection.SemanticPoint = FaultPointBeforeCommitMarker
		opt.FaultInjection.SemanticFailAfter = 0
		db, err := Open(opt)
		if err != nil {
			os.Exit(23)
		}
		err = db.PutBatch("crash", []KV{
			{Key: []byte("first"), Value: []byte("value-1")},
			{Key: []byte("second"), Value: []byte("value-2")},
		}, Persistent)
		if !errors.Is(err, ErrFaultInjectedSemantic) {
			os.Exit(24)
		}
		os.Exit(97)
	case "gc-prepared", "gc-pointers-installed", "gc-before-source-remove", "gc-source-removed", "gc-finalized":
		faults := map[string]string{
			"gc-prepared":             FaultPointGCPrepared,
			"gc-pointers-installed":   FaultPointGCPointersInstalled,
			"gc-before-source-remove": FaultPointGCBeforeSourceRemove,
			"gc-source-removed":       FaultPointGCSourceRemoved,
			"gc-finalized":            FaultPointGCFinalized,
		}
		opt.KVSeparation.Enable = true
		opt.KVSeparation.Threshold = 16
		opt.KVSeparation.ValueLogSegmentSize = 96
		db, err := Open(opt)
		if err != nil {
			os.Exit(26)
		}
		old := bytes.Repeat([]byte("o"), 40)
		live := bytes.Repeat([]byte("l"), 40)
		if err := db.Update(func(tx *Tx) error { return tx.Put("gc-subprocess", []byte("key"), old, Persistent) }); err != nil {
			os.Exit(27)
		}
		if err := db.Update(func(tx *Tx) error { return tx.Put("gc-subprocess", []byte("source-live"), old, Persistent) }); err != nil {
			os.Exit(28)
		}
		if err := db.Update(func(tx *Tx) error { return tx.Put("gc-subprocess", []byte("key"), live, Persistent) }); err != nil {
			os.Exit(29)
		}
		if err := db.Update(func(tx *Tx) error { return tx.Put("gc-subprocess", []byte("rotate"), live, Persistent) }); err != nil {
			os.Exit(30)
		}
		db.opt.FaultInjection.Enable = true
		db.opt.FaultInjection.SemanticPoint = faults[scenario]
		db.opt.FaultInjection.SemanticFailAfter = 0
		db.opt.faultState = &faultInjectionState{}
		if _, err := db.ValueLogGC(0.4); !errors.Is(err, ErrFaultInjectedSemantic) {
			os.Exit(31)
		}
		os.Exit(97)
	case "gc-metadata":
		occurrence, err := strconv.Atoi(os.Getenv(crashMetadataOccurrenceEnv))
		if err != nil || occurrence < 1 || occurrence > 5 {
			os.Exit(32)
		}
		opt.KVSeparation.Enable = true
		opt.KVSeparation.Threshold = 16
		opt.KVSeparation.ValueLogSegmentSize = 96
		db, err := Open(opt)
		if err != nil {
			os.Exit(33)
		}
		old := bytes.Repeat([]byte("o"), 40)
		live := bytes.Repeat([]byte("l"), 40)
		for _, item := range []struct {
			key   string
			value []byte
		}{
			{key: "key", value: old},
			{key: "source-live", value: old},
			{key: "key", value: live},
			{key: "rotate", value: live},
		} {
			if err := db.Update(func(tx *Tx) error {
				return tx.Put("gc-metadata-subprocess", []byte(item.key), item.value, Persistent)
			}); err != nil {
				os.Exit(34)
			}
		}
		db.opt.FaultInjection.Enable = true
		db.opt.FaultInjection.MetadataStage = os.Getenv(crashMetadataStageEnv)
		db.opt.FaultInjection.MetadataFailAfter = int64(occurrence - 1)
		db.opt.faultState = &faultInjectionState{}
		if _, err := db.ValueLogGC(0.4); !errors.Is(err, ErrFaultInjectedMetadata) {
			os.Exit(35)
		}
		os.Exit(97)
	case "group-committed-concurrent":
		opt.SyncPolicy.Mode = SyncPolicyGroupCommit
		db, err := Open(opt)
		if err != nil {
			os.Exit(50)
		}
		var wg sync.WaitGroup
		errs := make([]error, groupSubprocessWriters)
		for w := 0; w < groupSubprocessWriters; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				errs[w] = db.Update(func(tx *Tx) error {
					return tx.Put("group-subprocess",
						[]byte(fmt.Sprintf("writer-%d", w)),
						[]byte(fmt.Sprintf("value-%d", w)), Persistent)
				})
			}(w)
		}
		wg.Wait()
		for _, e := range errs {
			if e != nil {
				os.Exit(51)
			}
		}
		os.Exit(97)
	case "group-before-commit-marker":
		opt.SyncPolicy.Mode = SyncPolicyGroupCommit
		opt.FaultInjection.Enable = true
		opt.FaultInjection.SemanticPoint = FaultPointBeforeCommitMarker
		opt.FaultInjection.SemanticFailAfter = 0
		db, err := Open(opt)
		if err != nil {
			os.Exit(52)
		}
		err = db.PutBatch("group-subprocess", []KV{
			{Key: []byte("first"), Value: []byte("value-1")},
			{Key: []byte("second"), Value: []byte("value-2")},
		}, Persistent)
		if !errors.Is(err, ErrFaultInjectedSemantic) {
			os.Exit(53)
		}
		os.Exit(97)
	case "merge-prepared", "merge-installed", "merge-before-source-remove":
		faults := map[string]string{
			"merge-prepared":             FaultPointMergePrepared,
			"merge-installed":            FaultPointMergeInstalled,
			"merge-before-source-remove": FaultPointMergeBeforeSourceRemove,
		}
		opt.SegmentSize = 128
		db, err := Open(opt)
		if err != nil {
			os.Exit(40)
		}
		value := bytes.Repeat([]byte("s"), 32)
		for _, key := range []string{"key", "other"} {
			if err := db.Update(func(tx *Tx) error {
				return tx.Put("merge-semantic-subprocess", []byte(key), value, Persistent)
			}); err != nil {
				os.Exit(41)
			}
		}
		db.opt.FaultInjection.Enable = true
		db.opt.FaultInjection.SemanticPoint = faults[scenario]
		db.opt.FaultInjection.SemanticFailAfter = 0
		db.opt.faultState = &faultInjectionState{}
		if err := db.Merge(); !errors.Is(err, ErrFaultInjectedSemantic) {
			os.Exit(42)
		}
		os.Exit(97)
	case "merge-metadata":
		occurrence, err := strconv.Atoi(os.Getenv(crashMetadataOccurrenceEnv))
		if err != nil || occurrence < 1 || occurrence > 2 {
			os.Exit(36)
		}
		opt.SegmentSize = 128
		db, err := Open(opt)
		if err != nil {
			os.Exit(37)
		}
		value := bytes.Repeat([]byte("m"), 32)
		for _, key := range []string{"key", "other"} {
			if err := db.Update(func(tx *Tx) error {
				return tx.Put("merge-metadata-subprocess", []byte(key), value, Persistent)
			}); err != nil {
				os.Exit(38)
			}
		}
		db.opt.FaultInjection.Enable = true
		db.opt.FaultInjection.MetadataStage = os.Getenv(crashMetadataStageEnv)
		db.opt.FaultInjection.MetadataFailAfter = int64(occurrence - 1)
		db.opt.faultState = &faultInjectionState{}
		if err := db.Merge(); !errors.Is(err, ErrFaultInjectedMetadata) {
			os.Exit(39)
		}
		os.Exit(97)
	default:
		os.Exit(25)
	}
}

func TestCrashExplorerSubprocessRecovery(t *testing.T) {
	tests := []struct {
		name      string
		committed bool
	}{
		{name: "committed-before-exit", committed: true},
		{name: "before-commit-marker"},
	}
	for _, tt := range tests {
		t.Run("subprocess/"+tt.name, func(t *testing.T) {
			dir := t.TempDir()
			runCrashChild(t, dir, tt.name)
			opt := DefaultOptions
			opt.Dir = dir
			opt.SegmentSize = 1024 * 1024
			db := openTestDB(t, opt)
			defer db.Close()
			if tt.committed {
				assertValue(t, db, "crash", []byte("committed"), []byte("value"))
				return
			}
			assertNotFound(t, db, "crash", []byte("first"))
			assertNotFound(t, db, "crash", []byte("second"))
		})
	}
}

func TestValueLogGCSubprocessRecovery(t *testing.T) {
	tests := []struct {
		name             string
		sourceMustRemain bool
	}{
		{name: "gc-prepared", sourceMustRemain: true},
		{name: "gc-pointers-installed"},
		{name: "gc-before-source-remove"},
		{name: "gc-source-removed"},
		{name: "gc-finalized"},
	}
	for _, tt := range tests {
		t.Run("subprocess/"+tt.name, func(t *testing.T) {
			dir := t.TempDir()
			runCrashChild(t, dir, tt.name)
			assertValueLogGCSubprocessRecovery(t, dir, tt.sourceMustRemain)

			opt := DefaultOptions
			opt.Dir = dir
			opt.SegmentSize = 1024 * 1024
			opt.SyncPolicy.Mode = SyncPolicyEveryCommit
			opt.KVSeparation.Enable = true
			opt.KVSeparation.Threshold = 16
			opt.KVSeparation.ValueLogSegmentSize = 96
			db := openTestDB(t, opt)
			defer db.Close()
			assertValue(t, db, "gc-subprocess", []byte("key"), bytes.Repeat([]byte("l"), 40))
			assertValue(t, db, "gc-subprocess", []byte("source-live"), bytes.Repeat([]byte("o"), 40))
		})
	}
}

func TestGroupCommitSubprocessRecovery(t *testing.T) {
	t.Run("subprocess/group-committed-concurrent", func(t *testing.T) {
		dir := t.TempDir()
		runCrashChild(t, dir, "group-committed-concurrent")
		opt := DefaultOptions
		opt.Dir = dir
		opt.SegmentSize = 1024 * 1024
		opt.SyncPolicy.Mode = SyncPolicyGroupCommit
		db := openTestDB(t, opt)
		defer db.Close()
		for w := 0; w < groupSubprocessWriters; w++ {
			assertValue(t, db, "group-subprocess",
				[]byte(fmt.Sprintf("writer-%d", w)),
				[]byte(fmt.Sprintf("value-%d", w)))
		}
	})
	t.Run("subprocess/group-before-commit-marker", func(t *testing.T) {
		dir := t.TempDir()
		runCrashChild(t, dir, "group-before-commit-marker")
		opt := DefaultOptions
		opt.Dir = dir
		opt.SegmentSize = 1024 * 1024
		db := openTestDB(t, opt)
		defer db.Close()
		assertNotFound(t, db, "group-subprocess", []byte("first"))
		assertNotFound(t, db, "group-subprocess", []byte("second"))
	})
}

func assertValueLogGCSubprocessRecovery(t testing.TB, dir string, sourceMustRemain bool) {
	t.Helper()
	opt := DefaultOptions
	opt.Dir = dir
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 96
	db, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	assertValueTB(t, db, "gc-subprocess", []byte("key"), bytes.Repeat([]byte("l"), 40))
	assertValueTB(t, db, "gc-subprocess", []byte("source-live"), bytes.Repeat([]byte("o"), 40))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, sourceErr := os.Stat(filepath.Join(dir, valueLogFileName))
	if sourceMustRemain && sourceErr != nil {
		t.Fatalf("referenced source removed: %v", sourceErr)
	}
	if !sourceMustRemain && !os.IsNotExist(sourceErr) {
		t.Fatalf("unreferenced source remains: %v", sourceErr)
	}
	if _, err := os.Stat(filepath.Join(dir, valueLogGCManifestName)); !os.IsNotExist(err) {
		t.Fatalf("manifest not cleared: %v", err)
	}
}

func TestMergeSemanticSubprocessRecovery(t *testing.T) {
	tests := []struct {
		name       string
		phase      mergeManifestPhase
		keepSource bool
	}{
		{name: "merge-prepared", phase: mergeManifestPrepared, keepSource: true},
		{name: "merge-installed", phase: mergeManifestInstalled},
		{name: "merge-before-source-remove", phase: mergeManifestInstalled},
	}
	for _, tt := range tests {
		t.Run("subprocess/"+tt.name, func(t *testing.T) {
			dir := t.TempDir()
			runCrashChild(t, dir, tt.name)
			manifest, err := readMergeManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Phase != tt.phase {
				t.Fatalf("phase %d want %d", manifest.Phase, tt.phase)
			}
			if _, err := os.Stat(getDataFilePath(dir, manifest.SourceFileID)); err != nil {
				t.Fatalf("source missing before recovery: %v", err)
			}
			if tt.phase == mergeManifestPrepared && manifest.FirstTargetFileID <= manifest.LastTargetFileID {
				t.Fatalf("prepared target range is not empty: %+v", manifest)
			}
			if tt.phase == mergeManifestInstalled {
				if manifest.FirstTargetFileID > manifest.LastTargetFileID {
					t.Fatalf("installed target range is empty: %+v", manifest)
				}
				for id := manifest.FirstTargetFileID; id <= manifest.LastTargetFileID; id++ {
					if _, err := os.Stat(getDataFilePath(dir, id)); err != nil {
						t.Fatalf("target %d missing before recovery: %v", id, err)
					}
				}
			}

			opt := DefaultOptions
			opt.Dir = dir
			opt.SegmentSize = 128
			opt.SyncPolicy.Mode = SyncPolicyEveryCommit
			db := openTestDB(t, opt)
			value := bytes.Repeat([]byte("s"), 32)
			assertValue(t, db, "merge-semantic-subprocess", []byte("key"), value)
			assertValue(t, db, "merge-semantic-subprocess", []byte("other"), value)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			_, sourceErr := os.Stat(getDataFilePath(dir, manifest.SourceFileID))
			if tt.keepSource && sourceErr != nil {
				t.Fatalf("prepared source removed: %v", sourceErr)
			}
			if !tt.keepSource && !os.IsNotExist(sourceErr) {
				t.Fatalf("installed source remains: %v", sourceErr)
			}
			if _, err := os.Stat(filepath.Join(dir, mergeManifestName)); !os.IsNotExist(err) {
				t.Fatalf("manifest not cleared: %v", err)
			}
		})
	}
}

func TestManifestMetadataSubprocessRecoveryMatrix(t *testing.T) {
	scenarios := faultmodel.EnumerateManifestFaultScenarios()
	if len(scenarios) != 28 {
		t.Fatalf("pure metadata model has %d scenarios want 28", len(scenarios))
	}
	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.ID(), func(t *testing.T) {
			dir := t.TempDir()
			stage := metadataStage(scenario.Stage)
			switch scenario.Kind {
			case faultmodel.ManifestMerge:
				runCrashChildWithMetadata(t, dir, "merge-metadata", stage, scenario.Occurrence)
				assertMergeMetadataSubprocessRecovery(t, dir)
			case faultmodel.ManifestValueGC:
				runCrashChildWithMetadata(t, dir, "gc-metadata", stage, scenario.Occurrence)
				assertGCMetadataSubprocessRecovery(t, dir, scenario.Occurrence)
			default:
				t.Fatalf("unknown manifest kind %q", scenario.Kind)
			}
		})
	}
}

func assertGCMetadataSubprocessRecovery(t testing.TB, dir string, occurrence int) {
	t.Helper()
	opt := DefaultOptions
	opt.Dir = dir
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 96
	db, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	assertValueTB(t, db, "gc-metadata-subprocess", []byte("key"), bytes.Repeat([]byte("l"), 40))
	assertValueTB(t, db, "gc-metadata-subprocess", []byte("source-live"), bytes.Repeat([]byte("o"), 40))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, sourceErr := os.Stat(filepath.Join(dir, valueLogFileName))
	if occurrence <= 2 && sourceErr != nil {
		t.Fatalf("source removed before pointer installation: %v", sourceErr)
	}
	if occurrence >= 3 && !os.IsNotExist(sourceErr) {
		t.Fatalf("unreferenced source remains: %v", sourceErr)
	}
	if _, err := os.Stat(filepath.Join(dir, valueLogGCManifestName)); !os.IsNotExist(err) {
		t.Fatalf("GC manifest not cleared: %v", err)
	}
}

func assertMergeMetadataSubprocessRecovery(t testing.TB, dir string) {
	t.Helper()
	opt := DefaultOptions
	opt.Dir = dir
	opt.SegmentSize = 128
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	db, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("m"), 32)
	assertValueTB(t, db, "merge-metadata-subprocess", []byte("key"), value)
	assertValueTB(t, db, "merge-metadata-subprocess", []byte("other"), value)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, mergeManifestName)); !os.IsNotExist(err) {
		t.Fatalf("merge manifest not cleared: %v", err)
	}
}

func assertValueTB(t testing.TB, db *DB, bucket string, key, want []byte) {
	t.Helper()
	if err := db.View(func(tx *Tx) error {
		entry, err := tx.Get(bucket, key)
		if err != nil {
			return err
		}
		if !bytes.Equal(entry.Value, want) {
			return fmt.Errorf("got %q want %q", entry.Value, want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func runCrashChild(t testing.TB, dir, scenario string) {
	runCrashChildWithMetadata(t, dir, scenario, "", 0)
}

func runCrashChildWithMetadata(t testing.TB, dir, scenario string, stage metadataStage, occurrence int) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestCrashExplorerSubprocessChild$")
	command.Env = append(os.Environ(),
		crashChildEnv+"=1",
		crashDirEnv+"="+dir,
		crashScenarioEnv+"="+scenario,
		crashMetadataStageEnv+"="+string(stage),
		crashMetadataOccurrenceEnv+"="+strconv.Itoa(occurrence),
	)
	err := command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 97 {
		t.Fatalf("scenario subprocess/%s child got %v", scenario, err)
	}
}

func BenchmarkMergeSemanticSubprocessRecovery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(b.TempDir(), fmt.Sprintf("merge-semantic-%06d", i))
		if err := os.MkdirAll(dir, 0755); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		runCrashChild(b, dir, "merge-installed")
		opt := DefaultOptions
		opt.Dir = dir
		opt.SegmentSize = 128
		opt.SyncPolicy.Mode = SyncPolicyEveryCommit
		db, err := Open(opt)
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManifestMetadataSubprocessRecovery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(b.TempDir(), fmt.Sprintf("metadata-case-%06d", i))
		if err := os.MkdirAll(dir, 0755); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		runCrashChildWithMetadata(b, dir, "gc-metadata", metadataStageDirSync, 3)
		opt := DefaultOptions
		opt.Dir = dir
		opt.SegmentSize = 1024 * 1024
		opt.SyncPolicy.Mode = SyncPolicyEveryCommit
		opt.KVSeparation.Enable = true
		opt.KVSeparation.Threshold = 16
		opt.KVSeparation.ValueLogSegmentSize = 96
		db, err := Open(opt)
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueLogGCSubprocessRecovery(b *testing.B) {
	cases := []struct {
		name       string
		sourceLive bool
	}{
		{name: "gc-prepared", sourceLive: true},
		{name: "gc-pointers-installed"},
		{name: "gc-before-source-remove"},
		{name: "gc-source-removed"},
		{name: "gc-finalized"},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := filepath.Join(b.TempDir(), fmt.Sprintf("gc-case-%s-%06d", tc.name, i))
				if err := os.MkdirAll(dir, 0755); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				runCrashChild(b, dir, tc.name)
				assertValueLogGCSubprocessRecovery(b, dir, tc.sourceLive)
			}
		})
	}
}

func BenchmarkGroupCommitSubprocessRecovery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(b.TempDir(), fmt.Sprintf("group-case-%06d", i))
		if err := os.MkdirAll(dir, 0755); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		runCrashChild(b, dir, "group-committed-concurrent")
		opt := DefaultOptions
		opt.Dir = dir
		opt.SegmentSize = 1024 * 1024
		opt.SyncPolicy.Mode = SyncPolicyGroupCommit
		db, err := Open(opt)
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSubprocessCrashRecovery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(b.TempDir(), fmt.Sprintf("case-%06d", i))
		if err := os.MkdirAll(dir, 0755); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		runCrashChild(b, dir, "before-commit-marker")
		opt := DefaultOptions
		opt.Dir = dir
		opt.SegmentSize = 1024 * 1024
		db, err := Open(opt)
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
