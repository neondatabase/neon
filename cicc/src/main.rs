use std::{io::Write, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use glob::glob;
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(rename_all = "kebab-case")]
struct Cli {
    directory: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum BuildSystem {
    #[serde(rename = "pgxs")]
    PGXS,
    #[serde(rename = "cmake-ninja")]
    CmakeNinja,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ExtensionManifest {
    pub name: String,
    pub version: String,
    pub build_system: BuildSystem,
    #[serde(default)]
    pub build_arguments: Vec<String>,
    pub tarball: String,
    pub checksum: String,
    pub trusted: bool,
}

impl TryFrom<PathBuf> for ExtensionManifest {
    type Error = anyhow::Error;

    fn try_from(path: PathBuf) -> std::result::Result<Self, Self::Error> {
        let content = std::fs::read_to_string(path)?;

        let manifest: ExtensionManifest = toml::from_str(&content)?;

        Ok(manifest)
    }
}

impl ExtensionManifest {
    pub fn compile(self) {
        let mut stdout = std::io::stdout().lock();

        writeln!(
            &mut stdout,
            r"FROM build-deps AS {name}-src
ARG PG_VERSION

WORKDIR /ext-src
RUN wget '{tarball}' -O '{name}.tar.gz' && \
    echo '{checksum} {name}.tar.gz' | sha256sum --check && \
    mkdir '{name}-src' && cd '{name}-src' && tar xzf '../{name}.tar.gz' --strip-components=1 -C .

FROM pg-build AS {name}-build
COPY --from={name}-src /ext-src/ /ext-src/
WORKDIR /ext-src/{name}-src
",
            name = self.name,
            tarball = self.tarball,
            checksum = self.checksum
        )
        .unwrap();

        match self.build_system {
            BuildSystem::PGXS => writeln!(
                &mut stdout,
                r#"RUN make -j "$(getconf _NPROCESSORS_ONLN)" install && \"#,
            )
            .unwrap(),
            BuildSystem::CmakeNinja => writeln!(
                &mut stdout,
                r"RUN cmake -G Ninja -B build -DCMAKE_BUILD_TYPE=Release {build_args}
    ninja -C install && \",
                build_args = self.build_arguments.join(" ")
            )
            .unwrap(),
        }

        if self.trusted {
            writeln!(
                &mut stdout,
                "    echo 'trusted = true' >> '/usr/local/pgsql/share/extension/{name}.control",
                name = self.name
            )
            .unwrap();
        } else {
            writeln!(&mut stdout, "    true").unwrap();
        }

        write!(&mut stdout, "\n").unwrap();
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DependencyManifest {}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if !cli.directory.exists() {
        eprintln!("Directory does not exist");

        std::process::exit(1);
    }

    for entry in glob(cli.directory.join("*.toml").to_str().unwrap()).unwrap() {
        let path = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        let manifest: ExtensionManifest = match path.clone().try_into() {
            Ok(manifest) => manifest,
            Err(e) => {
                eprintln!("Failed to read {}: {}", path.display(), e);
                std::process::exit(1);
            }
        };

        manifest.compile();
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn verify_cli() {
        Cli::command().debug_assert()
    }
}
