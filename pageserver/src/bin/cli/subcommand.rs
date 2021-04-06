use anyhow::Result;

/// All subcommands need to implement this interface.
pub trait SubCommand {
    /// Generates the cli-config that Clap requires for the subcommand.
    fn gen_clap_command(&self) -> clap::App;

    /// Runs the body of the subcommand.
    fn run(&self, args: clap::ArgMatches) -> Result<()>;
}

/// A struct which holds a vector of heap-allocated `Box`es of trait objects all of which must
/// implement the `SubCommand` trait, but other than that, can be of any type.
pub struct ClapCommands {
    pub commands: Vec<Box<dyn SubCommand>>,
}

impl ClapCommands {
    /// Generates a vector of `clap::Apps` that can be passed into clap's `.subcommands()` method in
    /// order to generate the full CLI.
    pub fn generate(&self) -> Vec<clap::App> {
        let mut v: Vec<clap::App> = Vec::new();

        for command in self.commands.iter() {
            v.push(command.gen_clap_command());
        }
        v
    }
}
