package example

import (
	"testing"
	"time"
)

func TestEcommerceScenarioPersistsInventoryOrdersAndSessions(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.PutProduct(Product{SKU: "sku-book", Name: "KV Storage Book", Stock: 10}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateSession(Session{User: "alice", Token: "token-alice"}, 60); err != nil {
		t.Fatal(err)
	}
	if err := store.PlaceOrder(Order{ID: "order-001", User: "alice", SKU: "sku-book", Count: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.PlaceOrder(Order{ID: "order-002", User: "alice", SKU: "sku-book", Count: 2}); err != nil {
		t.Fatal(err)
	}

	product, err := store.Product("sku-book")
	if err != nil {
		t.Fatal(err)
	}
	if product.Stock != 5 {
		t.Fatalf("stock got %d want 5", product.Stock)
	}
	orders, err := store.OrdersByUser("alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 2 || orders[0].ID != "order-001" || orders[1].ID != "order-002" {
		t.Fatalf("unexpected orders: %#v", orders)
	}
	session, err := store.Session("token-alice")
	if err != nil {
		t.Fatal(err)
	}
	if session.User != "alice" {
		t.Fatalf("session user got %q want alice", session.User)
	}
	if store.Stats().ValidKeyCount != 6 {
		t.Fatalf("valid keys got %d want 6", store.Stats().ValidKeyCount)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	product, err = store.Product("sku-book")
	if err != nil {
		t.Fatal(err)
	}
	if product.Stock != 5 {
		t.Fatalf("reopen stock got %d want 5", product.Stock)
	}
	orders, err = store.OrdersByUser("alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 2 || orders[0].Count != 3 || orders[1].Count != 2 {
		t.Fatalf("unexpected reopened orders: %#v", orders)
	}
}

func TestEcommerceScenarioRejectsOversell(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.PutProduct(Product{SKU: "sku-limited", Name: "Limited Item", Stock: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.PlaceOrder(Order{ID: "order-too-large", User: "bob", SKU: "sku-limited", Count: 2}); err == nil {
		t.Fatal("expected oversell to fail")
	}
	product, err := store.Product("sku-limited")
	if err != nil {
		t.Fatal(err)
	}
	if product.Stock != 1 {
		t.Fatalf("stock got %d want unchanged 1", product.Stock)
	}
	orders, err := store.OrdersByUser("bob")
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 0 {
		t.Fatalf("unexpected orders after failed checkout: %#v", orders)
	}
}

func TestEcommerceScenarioExpiresSessions(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.CreateSession(Session{User: "alice", Token: "short-lived"}, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if _, err := store.Session("short-lived"); err == nil {
		t.Fatal("expected session to expire")
	}
}
