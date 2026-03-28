package execution

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/TFMV/duckkernel/internal/execution/materialize"
	"github.com/TFMV/duckkernel/internal/execution/runtime"
)

func Example_executionPlan() {
	logger := log.New(os.Stderr, "[runtime] ", log.LstdFlags)
	rt, err := runtime.New(":memory:", logger, true)
	if err != nil {
		fmt.Printf("Failed to create runtime: %v\n", err)
		return
	}
	defer rt.Close()

	ctx := context.Background()

	plan := runtime.ExecutionPlan{
		Nodes: []runtime.PlanNode{
			{
				NodeID:       "users",
				SQL:          "SELECT 1 as id, 'Alice' as name, true as active UNION ALL SELECT 2, 'Bob', true UNION ALL SELECT 3, 'Charlie', false",
				Action:       runtime.ActionCompute,
				Mode:         materialize.Ephemeral,
				Dependencies: []string{},
			},
			{
				NodeID:       "active_users",
				SQL:          "SELECT * FROM dk_users WHERE active = true",
				Action:       runtime.ActionCompute,
				Mode:         materialize.Ephemeral,
				Dependencies: []string{"users"},
			},
			{
				NodeID:       "enriched_users",
				SQL:          "SELECT u.*, CASE WHEN u.id = 1 THEN 'VIP' ELSE 'regular' as tier FROM dk_active_users u",
				Action:       runtime.ActionMaterialize,
				Mode:         materialize.Cached,
				Dependencies: []string{"active_users"},
			},
		},
	}

	result, err := rt.ExecutePlan(ctx, plan)
	if err != nil {
		fmt.Printf("Execution failed: %v\n", err)
		return
	}

	fmt.Printf("Total nodes: %d\n", result.TotalNodes)
	for _, r := range result.Results {
		fmt.Printf("Node: %s, Action: %s, Duration: %v\n", r.NodeID, r.Action, r.Duration)
	}
	fmt.Println("Execution completed successfully!")
}

func Example_streaming() {
	logger := log.New(os.Stderr, "[runtime] ", log.LstdFlags)
	rt, err := runtime.New(":memory:", logger, false)
	if err != nil {
		fmt.Printf("Failed to create runtime: %v\n", err)
		return
	}
	defer rt.Close()

	ctx := context.Background()

	plan := runtime.ExecutionPlan{
		Nodes: []runtime.PlanNode{
			{
				NodeID: "analytics",
				SQL:    "SELECT * FROM range(10000) AS t(id)",
				Action: runtime.ActionCompute,
			},
		},
	}

	if _, err := rt.ExecutePlan(ctx, plan); err != nil {
		fmt.Printf("Execution failed: %v\n", err)
		return
	}

	stream, err := rt.StreamNode(ctx, "analytics")
	if err != nil {
		fmt.Printf("Stream failed: %v\n", err)
		return
	}
	defer stream.Close()

	cols, _ := stream.Columns()
	fmt.Printf("Columns: %v\n", cols)

	rowCount := 0
	limit := 100
	for stream.Next() && rowCount < limit {
		record := stream.Record()
		if record == nil {
			break
		}
		fmt.Printf("Row %d: %v\n", rowCount, record)
		rowCount++
	}

	if err := stream.Err(); err != nil {
		fmt.Printf("Stream error: %v\n", err)
		return
	}

	fmt.Printf("Displayed %d rows (streaming without loading entire dataset)\n", rowCount)
}

func Example_failureHandling() {
	logger := log.New(os.Stderr, "[runtime] ", log.LstdFlags)
	rt, err := runtime.New(":memory:", logger, true)
	if err != nil {
		fmt.Printf("Failed to create runtime: %v\n", err)
		return
	}
	defer rt.Close()

	ctx := context.Background()

	plan := runtime.ExecutionPlan{
		Nodes: []runtime.PlanNode{
			{
				NodeID: "users",
				SQL:    "SELECT 1 as id",
				Action: runtime.ActionCompute,
			},
			{
				NodeID: "broken",
				SQL:    "SELECT * FROM nonexistent_table",
				Action: runtime.ActionCompute,
			},
			{
				NodeID: "downstream",
				SQL:    "SELECT * FROM dk_users",
				Action: runtime.ActionCompute,
			},
		},
	}

	result, err := rt.ExecutePlan(ctx, plan)
	if err != nil {
		fmt.Printf("Execution halted as expected: %v\n", err)
	}

	fmt.Printf("Results after failure:\n")
	for _, r := range result.Results {
		status := "success"
		if r.Error != nil {
			status = fmt.Sprintf("failed: %v", r.Error)
		}
		fmt.Printf("  %s: %s\n", r.NodeID, status)
	}

	fmt.Printf("Failed: %v\n", result.Failed)
}

func Example_cleanup() {
	logger := log.New(os.Stderr, "[runtime] ", log.LstdFlags)
	rt, err := runtime.New(":memory:", logger, false)
	if err != nil {
		fmt.Printf("Failed to create runtime: %v\n", err)
		return
	}
	defer rt.Close()

	ctx := context.Background()

	plan := runtime.ExecutionPlan{
		Nodes: []runtime.PlanNode{
			{
				NodeID: "temp_data",
				SQL:    "SELECT * FROM range(10) AS t(id)",
				Action: runtime.ActionCompute,
				Mode:   materialize.Ephemeral,
			},
		},
	}

	if _, err := rt.ExecutePlan(ctx, plan); err != nil {
		fmt.Printf("Execution failed: %v\n", err)
		return
	}

	fmt.Println("Before cleanup:")
	stream, _ := rt.StreamNode(ctx, "temp_data")
	for stream.Next() {
		stream.Record()
	}
	stream.Close()

	if err := rt.Cleanup("temp_data", materialize.Ephemeral); err != nil {
		fmt.Printf("Cleanup error: %v\n", err)
	}

	fmt.Println("After cleanup - table dropped")
}

func Example_materializationModes() {
	logger := log.New(os.Stderr, "[runtime] ", log.LstdFlags)
	rt, err := runtime.New(":memory:", logger, false)
	if err != nil {
		fmt.Printf("Failed to create runtime: %v\n", err)
		return
	}
	defer rt.Close()

	ctx := context.Background()

	modes := []materialize.MaterializationMode{
		materialize.Ephemeral,
		materialize.Cached,
		materialize.Persistent,
	}

	for _, mode := range modes {
		plan := runtime.ExecutionPlan{
			Nodes: []runtime.PlanNode{
				{
					NodeID: "test",
					SQL:    "SELECT 1 as val",
					Action: runtime.ActionMaterialize,
					Mode:   mode,
				},
			},
		}

		result, err := rt.ExecutePlan(ctx, plan)
		if err != nil {
			fmt.Printf("Mode %s: error: %v\n", mode, err)
			continue
		}
		fmt.Printf("Mode %s: completed in %v\n", mode, result.Results[0].Duration)
	}
}

func Example_executionPlanComplete() {
	Example_executionPlan()
	Example_streaming()
	Example_failureHandling()
	Example_cleanup()
	Example_materializationModes()
}
