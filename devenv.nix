{ pkgs, ... }: {
  languages.rust = {
    enable = true;
    channel = "stable";
    components = [ "rustc" "cargo" "clippy" "rustfmt" "rust-analyzer" ];
  };

  languages.python = {
    enable = true;
    package = pkgs.python311Packages.python;
    venv = {
      enable = true;
      requirements = ''
        maturin
      '';
    };
  };

  packages = with pkgs; [
    cargo-expand
  ];
}
