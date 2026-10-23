namespace dXn.SignalR.AppHost;

/// <summary>
/// The multithreaded (atomics) browser WASM build RayX ships, reproduced for the fixture and the
/// threaded platform case. The flags mirror RayX <c>xtask/src/wasm.rs</c>.
/// </summary>
public static class ThreadedWasm
{
  /// <summary>The pinned nightly toolchain (needs the <c>rust-src</c> component and the wasm target).</summary>
  public const string Toolchain = "nightly-2026-06-23";

  /// <summary>RayX's threaded <c>RUSTFLAGS</c>: atomics, shared imported memory and TLS exports.</summary>
  public const string RustFlags =
    "-C target-feature=+atomics,+bulk-memory,+mutable-globals -C link-arg=--shared-memory "
    + "-C link-arg=--import-memory -C link-arg=--max-memory=2147483648 -C link-arg=--export=__heap_base "
    + "-C link-arg=--export=__wasm_init_tls -C link-arg=--export=__tls_size -C link-arg=--export=__tls_align "
    + "-C link-arg=--export=__tls_base -Awarnings";

  /// <summary>The <c>cargo build</c> arguments for a threaded wasm32 build.</summary>
  /// <param name="manifest">The manifest path.</param>
  /// <param name="targetDirectory">The target directory (kept separate because the flags differ).</param>
  /// <param name="extra">Extra arguments, such as feature selection.</param>
  /// <returns>The arguments after <c>cargo</c>.</returns>
  public static string[] BuildArguments(string manifest, string targetDirectory, params string[] extra) =>
  [
    $"+{Toolchain}",
    "build",
    "--manifest-path",
    manifest,
    "--target",
    "wasm32-unknown-unknown",
    "-Z",
    "build-std=std,panic_abort",
    "--target-dir",
    targetDirectory,
    .. extra
  ];
}
