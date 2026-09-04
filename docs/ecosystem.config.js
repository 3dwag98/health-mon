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
      },

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
