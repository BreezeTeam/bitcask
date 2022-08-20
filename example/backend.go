package example

import "github.com/BreezeTeam/bitcask/engine"

type Object struct {
	Bucket string
	Key    string
	Value  []byte
	TTL    uint32
}

func (s *Store) PutObject(object Object) error {
	return s.db.Update(func(tx *bitcask.Tx) error {
		return tx.Put(object.Bucket, []byte(object.Key), object.Value, object.TTL)
	})
}

func (s *Store) PutObjects(objects []Object) error {
	return s.db.Update(func(tx *bitcask.Tx) error {
		for _, object := range objects {
			if err := tx.PutBatch(object.Bucket, []bitcask.KV{{Key: []byte(object.Key), Value: object.Value}}, object.TTL); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) Object(bucket, key string) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bitcask.Tx) error {
		entry, err := tx.Get(bucket, []byte(key))
		if err != nil {
			return err
		}
		value = append([]byte(nil), entry.Value...)
		return nil
	})
	return value, err
}
