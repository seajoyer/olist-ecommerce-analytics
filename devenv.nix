{ pkgs, lib, ... }:

{
  packages = with pkgs; [
    git
    pwgen
    docker-compose
    postgresql_18
  ];

  languages.python = {
    enable = false;
    version = "3.14";
    uv = {
      enable = false;
      sync.enable = true;
    };
    venv.enable = false;
  };

  services.postgres = {
    enable = true;
    package = pkgs.postgresql_18;
    port = 5432;
    listen_addresses = "*";
    initialDatabases = [
      { name = "project"; }
      { name = "redash"; }
    ];
    initialScript = ''
      # ALTER ROLE dmitry WITH PASSWORD 'redashpass';
    '';
    hbaConf = ''
      # Local Unix socket connections (passwordless, OS user matching or trust)
      local   all             all                                     trust

      # IPv4 loopback (localhost)
      host    all             all             127.0.0.1/32            trust

      # IPv6 loopback
      host    all             all             ::1/128                 trust

      # All other connections (including from Docker containers) - trust for local dev
      host    all             all             0.0.0.0/0               trust
      host    all             all             ::/0                    trust
    '';
  };

  processes.postgres = {
    ready = {
      exec = lib.mkForce "nc -z 127.0.0.1 5432 || exit 1";
    };
  };

  dotenv.enable = true;

  # Redis (required by Redash)
  services.redis = {
    enable = true;
    bind = "0.0.0.0";
    port = 6379;
  };
}
