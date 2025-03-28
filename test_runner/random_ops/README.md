# Random Operations Test for Neon Stability

## Problem Statement

Neon needs robust testing of Neon's stability to ensure reliability for users. The random operations test addresses this by continuously exercising the API with unpredictable sequences of operations, helping to identify edge cases and potential issues that might not be caught by deterministic tests.

### Key Components

#### 1. Class Structure

The test implements three main classes to model the Neon architecture:

- **NeonProject**: Represents a Neon project and manages the lifecycle of branches and endpoints
- **NeonBranch**: Represents a branch within a project, with methods for creating child branches, endpoints, and performing point-in-time restores
- **NeonEndpoint**: Represents an endpoint (connection point) for a branch, with methods for managing benchmarks

#### 2. Operations Tested

The test randomly performs the following operations with weighted probabilities:

- **Creating branches** 
- **Deleting branches**
- **Adding read-only endpoints**
- **Deleting read-only endpoints**
- **Restoring branches to random points in time**

#### 3. Load Generation

Each branch and endpoint is loaded with `pgbench` to simulate real database workloads during testing. This ensures that the operations are performed against branches with actual data and ongoing transactions.

#### 4. Error Handling

The test includes robust error handling for various scenarios:
- Branch limit exceeded
- Connection timeouts
- Control plane timeouts (HTTP 524 errors)
- Benchmark failures

#### 5. CI Integration

The test is integrated into the CI pipeline via a GitHub workflow that runs daily, ensuring continuous validation of API stability.

## How It Works

1. The test creates a Neon project using the Public API
2. It initializes the main branch with pgbench data
3. It performs random operations according to the weighted probabilities
4. During each operation, it checks that all running benchmarks are still operational
5. The test cleans up by deleting the project at the end

## Configuration

The test can be configured with:
- `RANDOM_SEED`: Set a specific random seed for reproducible test runs
- `NEON_API_KEY`: API key for authentication
- `NEON_API_BASE_URL`: Base URL for the API (defaults to staging environment)
- `NUM_OPERATIONS`: The number of operations to be performed

## Running the Test

The test is designed to run in the CI environment but can also be executed locally:

```bash
NEON_API_KEY=your_api_key ./scripts/pytest test_runner/random_ops/test_random_ops.py -m remote_cluster
```

To run with a specific random seed for reproducibility:

```bash
RANDOM_SEED=12345 NEON_API_KEY=your_api_key ./scripts/pytest test_runner/random_ops/test_random_ops.py -m remote_cluster
```

To run with the custom number of operations:

```bash
NUM_OPERATIONS=500 NEON_API_KEY=your_api_key ./scripts/pytest test_runner/random_ops/test_random_ops.py -m remote_cluster
```

## Benefits

This test provides several key benefits:
1. **Comprehensive API testing**: Exercises multiple API endpoints in combination
2. **Edge case discovery**: Random sequences may uncover issues not found in deterministic tests
3. **Stability validation**: Continuous execution helps ensure long-term API reliability
4. **Regression prevention**: Detects if new changes break existing API functionality

## Future Improvements

Potential enhancements to the test could include:
1. Adding more API operations, e.g. `reset_to_parent`, `snapshot`, etc 
2. Implementing more sophisticated load patterns
3. Adding metrics collection to measure API performance
4. Extending test duration for longer-term stability validation