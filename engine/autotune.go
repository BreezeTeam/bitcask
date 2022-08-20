package bitcask

import "errors"

var ErrAutonomousApplyDisabled = errors.New("autonomous policy application is disabled")

type AutoTuneResult struct {
	Recommendation    PolicyRecommendation
	CompactionChanged bool
	PlacementChanged  bool
	CompactionMode    CompactionPolicyMode
	LifecycleEnabled  bool
	Reason            string
}

func (db *DB) ApplyPolicyRecommendation() (AutoTuneResult, error) {
	options := db.opt.Autonomous
	if !options.ApplyCompaction && !options.ApplyKVPlacement {
		return AutoTuneResult{}, ErrAutonomousApplyDisabled
	}
	recommendation := db.PolicyRecommendation()
	result := AutoTuneResult{Recommendation: recommendation}
	if !recommendation.Available {
		result.Reason = "recommendation unavailable"
		return result, nil
	}
	minConfidence := options.MinConfidence
	if minConfidence <= 0 {
		minConfidence = 0.7
	}
	if recommendation.Confidence < minConfidence {
		result.Reason = "recommendation below confidence threshold"
		return result, nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if options.ApplyCompaction {
		mode := db.configuredCompactionMode
		switch recommendation.Compaction {
		case "garbage-ratio":
			mode = CompactionByGarbageRatio
		case "hot-cold":
			mode = CompactionHotCold
		case "file-id":
			mode = CompactionByFileID
		}
		if db.opt.Compaction.Mode != mode {
			db.opt.Compaction.Mode = mode
			result.CompactionChanged = true
		}
	}
	if options.ApplyKVPlacement && db.valueLog != nil {
		enabled := db.configuredLifecycle
		switch recommendation.Placement {
		case "lifecycle":
			enabled = true
		case "inline", "value-log":
			enabled = false
		}
		if enabled && db.lifecycle == nil {
			db.lifecycle = newLifecyclePlacement()
		}
		if db.opt.KVSeparation.LifecycleEnable != enabled {
			db.opt.KVSeparation.LifecycleEnable = enabled
			result.PlacementChanged = true
		}
	}
	result.CompactionMode = db.opt.Compaction.Mode
	result.LifecycleEnabled = db.opt.KVSeparation.LifecycleEnable
	result.Reason = recommendation.Reason
	if db.autonomous != nil {
		db.autonomous.mu.Lock()
		db.autonomous.appendAuditLocked(PolicyAuditEvent{
			Kind:              "application",
			Phase:             recommendation.Phase,
			Confidence:        recommendation.Confidence,
			CompactionChanged: result.CompactionChanged,
			PlacementChanged:  result.PlacementChanged,
			Reason:            result.Reason,
		})
		db.autonomous.mu.Unlock()
	}
	return result, nil
}

func (db *DB) ResetAutonomousPolicies() AutoTuneResult {
	db.mu.Lock()
	defer db.mu.Unlock()
	result := AutoTuneResult{
		CompactionChanged: db.opt.Compaction.Mode != db.configuredCompactionMode,
		PlacementChanged:  db.opt.KVSeparation.LifecycleEnable != db.configuredLifecycle,
		CompactionMode:    db.configuredCompactionMode,
		LifecycleEnabled:  db.configuredLifecycle,
		Reason:            "restored configured policies",
	}
	db.opt.Compaction.Mode = db.configuredCompactionMode
	db.opt.KVSeparation.LifecycleEnable = db.configuredLifecycle
	return result
}
