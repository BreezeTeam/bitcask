package example

import (
	"github.com/BreezeTeam/bitcask/engine"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	InventoryBucket = "inventory"
	OrderBucket     = "orders"
	SessionBucket   = "sessions"
	UserOrderBucket = "user_orders"
)

type Store struct {
	db *bitcask.DB
}

type Product struct {
	SKU   string `json:"sku"`
	Name  string `json:"name"`
	Stock int    `json:"stock"`
}

type Order struct {
	ID    string `json:"id"`
	User  string `json:"user"`
	SKU   string `json:"sku"`
	Count int    `json:"count"`
}

type Session struct {
	User  string `json:"user"`
	Token string `json:"token"`
}

func OpenStore(dir string) (*Store, error) {
	opt := bitcask.DefaultOptions
	opt.Dir = dir
	opt.SegmentSize = 1024 * 1024
	return OpenStoreWithOptions(opt)
}

func OpenStoreWithOptions(opt bitcask.Options) (*Store, error) {
	db, err := bitcask.Open(opt)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) PutProduct(product Product) error {
	return s.db.Update(func(tx *bitcask.Tx) error {
		value, err := json.Marshal(product)
		if err != nil {
			return err
		}
		return tx.Put(InventoryBucket, []byte(product.SKU), value, bitcask.Persistent)
	})
}

func (s *Store) Product(sku string) (Product, error) {
	var product Product
	err := s.db.View(func(tx *bitcask.Tx) error {
		entry, err := tx.Get(InventoryBucket, []byte(sku))
		if err != nil {
			return err
		}
		return json.Unmarshal(entry.Value, &product)
	})
	return product, err
}

func (s *Store) CreateSession(session Session, ttlSeconds uint32) error {
	return s.db.Update(func(tx *bitcask.Tx) error {
		value, err := json.Marshal(session)
		if err != nil {
			return err
		}
		return tx.Put(SessionBucket, []byte(session.Token), value, ttlSeconds)
	})
}

func (s *Store) Session(token string) (Session, error) {
	var session Session
	err := s.db.View(func(tx *bitcask.Tx) error {
		entry, err := tx.Get(SessionBucket, []byte(token))
		if err != nil {
			return err
		}
		return json.Unmarshal(entry.Value, &session)
	})
	return session, err
}

func (s *Store) PlaceOrder(order Order) error {
	return s.db.Update(func(tx *bitcask.Tx) error {
		productEntry, err := tx.Get(InventoryBucket, []byte(order.SKU))
		if err != nil {
			return err
		}
		var product Product
		if err := json.Unmarshal(productEntry.Value, &product); err != nil {
			return err
		}
		if product.Stock < order.Count {
			return fmt.Errorf("insufficient stock for sku %s", order.SKU)
		}

		product.Stock -= order.Count
		productValue, err := json.Marshal(product)
		if err != nil {
			return err
		}
		orderValue, err := json.Marshal(order)
		if err != nil {
			return err
		}
		if err := tx.Put(InventoryBucket, []byte(product.SKU), productValue, bitcask.Persistent); err != nil {
			return err
		}
		if err := tx.Put(OrderBucket, []byte(order.ID), orderValue, bitcask.Persistent); err != nil {
			return err
		}
		return tx.Put(UserOrderBucket, []byte(userOrderKey(order.User, order.ID)), orderValue, bitcask.Persistent)
	})
}

func (s *Store) OrdersByUser(user string) ([]Order, error) {
	var orders []Order
	err := s.db.View(func(tx *bitcask.Tx) error {
		kvs, _, err := tx.Prefix(UserOrderBucket, []byte(user+":"), 0, bitcask.ScanNoLimit)
		if errors.Is(err, bitcask.ErrBucketNotFound) {
			orders = []Order{}
			return nil
		}
		if err != nil {
			return err
		}
		orders = make([]Order, 0, len(kvs))
		for _, kv := range kvs {
			var order Order
			if err := json.Unmarshal(kv.Value, &order); err != nil {
				return err
			}
			orders = append(orders, order)
		}
		return nil
	})
	return orders, err
}

func (s *Store) Stats() bitcask.DBStats {
	return s.db.Stats()
}

func userOrderKey(user, orderID string) string {
	return user + ":" + orderID
}
