package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"google.golang.org/grpc"
)

type User struct {
	Uid     string   `json:"uid,omitempty"`
	Name    string   `json:"name,omitempty"`
	Balance int      `json:"balance,omitempty"`
	Dtype   []string `json:"dgraph.type,omitempty"`
}

func main() {
	fmt.Println("Hello, World!")
	c := newClient()
	setup(c)
	runTxn(c)
}

func newClient() *dgo.Dgraph {
	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	d, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal("newClient", err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}
func setup(c *dgo.Dgraph) {
	var err error
	// Drop all data including schema from the dgraph instance. This is useful
	// for small examples such as this, since it puts dgraph into a clean
	// state.
	//err = c.Alter(context.Background(), &api.Operation{DropOp: api.Operation_ALL})
	if err != nil {
		log.Fatal("setup", err)
	}
	// Install a schema into dgraph. Accounts have a `name` and a `balance`.
	err = c.Alter(context.Background(), &api.Operation{
		Schema: `
			name: string @index(term) .
			balance: int .
			type user {
				name
				balance
			}
		`,
	})

	if err != nil {
		log.Fatal("setup", err)
	}
}

func runTxn(c *dgo.Dgraph) {
	txn := c.NewTxn()
	var ctx = context.Background()
	defer txn.Discard(ctx)

	const q = `
		{
			all(func: anyofterms(name, "Vikram Mali")) {
				uid
                name
				balance
				dgraph.type
			}
		}
	`
	resp, err := txn.Query(ctx, q)
	if err != nil {
		log.Fatal(err)
	}

	// <0x17> <name> "Alice" .
	// <0x17> <balance> 100 .
	fmt.Println(string(resp.Json))

	if resp.Metrics.NumUids["_total"] == 0 {

		p := User{
			Uid:     "_:alice",
			Name:    "Vikram Mali",
			Balance: 26,
			Dtype:   make([]string, 0),
		}
		p.Dtype = append(p.Dtype, "user")
		mu := &api.Mutation{}
		pb, err := json.Marshal(p)
		if err != nil {
			log.Fatal("json.Marshal", err)
		}
		mu.SetJson = pb
		inputresponse, err := txn.Mutate(ctx, mu)
		if err != nil {
			log.Fatal("Mutate", err)
		} else {
			txn.Commit(ctx)
			log.Println("inputresponse ", inputresponse)

			resp, err = c.NewReadOnlyTxn().Query(ctx, q)
			if err != nil {
				log.Fatal(err)
			} else {
				fmt.Println(string(resp.Json))
			}
		}
	} else {

		// After we get the balances, we have to decode them into structs so that
		// we can manipulate the data.
		var decode struct {
			All []User
		}

		if err := json.Unmarshal(resp.GetJson(), &decode); err != nil {
			log.Fatal(err)
		}

		d := map[string]string{"uid": decode.All[0].Uid}
		pb, err := json.Marshal(d)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("deleting ", string(pb))
		mu := &api.Mutation{
			DeleteJson: pb,
		}
		deleteresponse, err := txn.Mutate(ctx, mu)
		if err != nil {
			log.Fatal("DeleteEdges", err)
		} else {
			txn.Commit(ctx)
			log.Println("deleteresponse ", deleteresponse)
		}
		resp, err := c.NewReadOnlyTxn().Query(ctx, q)
		if err != nil {
			log.Fatal(err)
		}

		// <0x17> <name> "Alice" .
		// <0x17> <balance> 100 .
		fmt.Println(string(resp.Json))

	}
}
