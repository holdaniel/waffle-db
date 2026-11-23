package waffle_test

import (
	"fmt"
	"time"

	waffle "github.com/holdaniel/waffle-db"
)

type Order struct {
	Item  string
	Cents int64
}

// Composite key: ordered by customer, then time.
type OrderKey struct {
	Customer string
	At       time.Time
}

func Example() {
	db := waffle.OpenMemory() // or waffle.Open("shop.db", nil)
	defer db.Close()

	shop := db.Namespace("shop")
	orders := waffle.MustBucket[OrderKey, Order](shop, "orders")
	stock := waffle.MustBucket[string, int](shop, "stock")

	_ = stock.Put("waffle", 10)

	t0 := time.Date(2026, 11, 23, 9, 0, 0, 0, time.UTC)
	buy := func(who string, at time.Time) error {
		return db.Update(func(tx *waffle.Tx) error {
			if err := stock.In(tx).Update("waffle", func(n int, _ bool) (int, error) {
				if n == 0 {
					return 0, fmt.Errorf("sold out")
				}
				return n - 1, nil
			}); err != nil {
				return err
			}
			return orders.In(tx).Put(OrderKey{who, at}, Order{"waffle", 450})
		})
	}
	_ = buy("bob", t0.Add(time.Hour))
	_ = buy("alice", t0.Add(2*time.Hour))
	_ = buy("alice", t0)

	aliceOrders := orders.Prefix(OrderKey{Customer: "alice"}, 1)
	for k, o := range aliceOrders.Seq() {
		fmt.Println(k.Customer, k.At.Format(time.Kitchen), o.Item, o.Cents)
	}
	if err := aliceOrders.Err(); err != nil {
		fmt.Println("error:", err)
	}
	left, _, _ := stock.Get("waffle")
	fmt.Println("left:", left)

	// Output:
	// alice 9:00AM waffle 450
	// alice 11:00AM waffle 450
	// left: 7
}
