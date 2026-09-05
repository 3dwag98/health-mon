// PM2 example. Documentation only -- nothing in the package imports, shells
// out to, or depends on PM2. The process manager is the deployment's concern.
//
// The restart policy inside worker-health (if armed) only ever exits the
// process with `exit_code`; PM2 is what actually restarts it. The two compose
// rather than duplicate: worker-health decides WHEN a restart is warranted,
// PM2 decides how often it is allowed to happen.
module.exports = {
  apps: [
    {
      name: "billing-worker",
      script: "worker_billing.py",
      interpreter: "python3",
      cwd: "/srv/app/workers",
      instances: 4,
      exec_mode: "fork",           // Python workers are not Node cluster-able

      env: {
        SERVICE: "billing",
        // Port and instance id come from explicit configuration, never from
        // a PM2 variable: whether PM2 injects pm_id into the environment of
        // non-Node child processes is unverified on the target version, and
        // a health endpoint that silently fails to bind is worse than none.
        HEALTH_PORT: "8080",
        HEALTH_INSTANCE: "billing-1",

        RESTART_ENABLED: "false",  // opt in deliberately, per worker
        RESTART_AFTER_CYCLES: "5",
        RESTART_MIN_UPTIME: "120", // never restart a process that just booted

        // Telemetry is pushed, so the supervisor does not have to make this
        // process discoverable by anything except itself.
        HEALTH_OTEL_ENDPOINT: "http://otel-collector:4318",
        HEALTH_ENVIRONMENT: "production",
      },

      // PM2 watches /live and NEVER /ready. This is the load-bearing line
      // in the file.
      //
      //   /live  503 -> this process is wedged: its loop stopped turning, or
      //                 it is holding a backlog it has stopped consuming.
      //                 A restart is the actual remedy.
      //   /ready 503 -> a dependency it needs is down. Restarting does not
      //                 bring the dependency back; it converts one database
      //                 outage into forty crash-looping workers hammering a
      //                 database that is already in trouble, and destroys the
      //                 in-flight work each was holding.
      //
      // Point a load balancer at /ready. Point the supervisor at /live.
      health_check_url: "http://127.0.0.1:8080/live",
      health_check_grace_period: 30,

      // Back off rather than hammering a dependency that is already down.
      exp_backoff_restart_delay: 500,
      max_restarts: 10,
      min_uptime: "60s",
      kill_timeout: 30000,         // matches drain_timeout, so in-flight
                                   // messages finish before SIGKILL
      autorestart: true,
      max_memory_restart: "512M",
    },
  ],
};
