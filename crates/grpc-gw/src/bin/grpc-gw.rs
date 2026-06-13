//! `grpc-gw` binary — M1 introspection commands.
//!
//! Two subcommands are wired so far (the runnable gateway `serve` lands with
//! the proxy/server modules):
//!
//! - `grpc-gw routes` — print the resolved route table for a descriptor set.
//! - `grpc-gw check` — validate a descriptor set offline (CI gate), exiting
//!   non-zero on a route conflict or unresolved binding.
//!
//! Both run purely on the [`RouteTable`] and need no listening socket. See
//! `docs/design/grpc-gateway-design.md#introspection--validation`.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use grpc_gw::{BodySelector, RouteTable};

#[derive(Parser)]
#[command(name = "grpc-gw", about = "Dynamic gRPC↔JSON transcoding gateway")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the resolved route table for a descriptor set.
    Routes(RoutesArgs),
    /// Validate a descriptor set offline; exit non-zero on problems.
    Check(CheckArgs),
}

/// Shared options for loading a descriptor set into a route table.
#[derive(Args)]
struct LoadArgs {
    /// Path to a serialized `FileDescriptorSet` (`.pb`), built with
    /// `protoc --include_imports` so `google/api/annotations.proto` is present.
    #[arg(short, long)]
    descriptor: PathBuf,

    /// Do **not** synthesize default `POST /pkg.Svc/Method` bindings for
    /// methods lacking a `google.api.http` annotation.
    #[arg(long)]
    no_unbound_methods: bool,
}

impl LoadArgs {
    fn load(&self) -> Result<RouteTable, String> {
        let bytes = std::fs::read(&self.descriptor)
            .map_err(|e| format!("failed to read {}: {e}", self.descriptor.display()))?;
        RouteTable::build(&bytes, !self.no_unbound_methods).map_err(|e| e.to_string())
    }
}

#[derive(Args)]
struct RoutesArgs {
    #[command(flatten)]
    load: LoadArgs,

    /// Emit the route table as JSON instead of the human-readable table.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct CheckArgs {
    #[command(flatten)]
    load: LoadArgs,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Routes(args) => run_routes(args),
        Command::Check(args) => run_check(args),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}

fn run_routes(args: RoutesArgs) -> Result<(), String> {
    let table = args.load.load()?;

    if args.json {
        let json = serde_json::to_string_pretty(&table).map_err(|e| e.to_string())?;
        println!("{json}");
    } else {
        print_table(&table);
    }
    Ok(())
}

fn run_check(args: CheckArgs) -> Result<(), String> {
    let table = args.load.load()?;

    let mut problems = Vec::new();

    // Methods that resolved to zero bindings are unreachable. With unbound
    // defaults enabled this only happens for an annotated method whose rule
    // carried no `pattern`; with them disabled, every unannotated method is
    // intentionally unexposed and not a problem.
    if !args.load.no_unbound_methods {
        for route in &table.routes {
            if route.bindings.is_empty() {
                problems.push(format!(
                    "{} has a google.api.http rule with no pattern (unreachable)",
                    route.grpc_path
                ));
            }
        }
    }

    for conflict in table.conflicts() {
        problems.push(format!("route conflict: {conflict}"));
    }

    if problems.is_empty() {
        eprintln!(
            "ok: {} method(s), {} binding(s), no conflicts",
            table.routes.len(),
            table.binding_count()
        );
        Ok(())
    } else {
        Err(format!(
            "{} problem(s) found:\n  - {}",
            problems.len(),
            problems.join("\n  - ")
        ))
    }
}

/// Render the route table as aligned, human-readable lines.
fn print_table(table: &RouteTable) {
    if table.routes.is_empty() {
        println!("(no methods in descriptor set)");
        return;
    }

    for route in &table.routes {
        let stream = if route.server_streaming {
            " [server-streaming]"
        } else {
            ""
        };
        println!("{}{stream}", route.grpc_path);

        if route.bindings.is_empty() {
            println!("    (no HTTP binding — not exposed)");
            continue;
        }

        for binding in &route.bindings {
            let origin = if binding.synthesized {
                "default"
            } else {
                "annotated"
            };
            let body = match &binding.body {
                BodySelector::Wildcard => "body=*".to_owned(),
                BodySelector::None => "body=-".to_owned(),
                BodySelector::Field(f) => format!("body={f}"),
            };
            let resp = binding
                .response_body
                .as_ref()
                .map(|r| format!(" response_body={r}"))
                .unwrap_or_default();
            println!(
                "    {:<6} {}  ({origin}, {body}{resp})",
                binding.http_method, binding.http_path
            );
        }
    }
}
