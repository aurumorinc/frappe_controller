### Frappe Controller

A high-performance, event-driven orchestrator for the Frappe Framework. This app provides a continuous, rate-limited background controller that bypasses the default 4-minute cron scheduler. It allows developers to register custom hook-based events, manage dynamic execution limits, and dispatch workloads in real-time to Frappe’s native RQ workers, complete with execution logging and state management.

### Installation

You can install this app using the [bench](https://github.com/frappe/bench) CLI:

```bash
cd $PATH_TO_YOUR_BENCH
bench get-app $URL_OF_THIS_REPO --branch main
bench install-app frappe_controller
```

### Contributing

This app uses `pre-commit` for code formatting and linting. Please [install pre-commit](https://pre-commit.com/#installation) and enable it for this repository:

```bash
cd apps/frappe_controller
pre-commit install
```

Pre-commit is configured to use the following tools for checking and formatting your code:

- ruff
- eslint
- prettier
- pyupgrade

### License

mit
