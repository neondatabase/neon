//! Handling for launching postgres in a cgroup

use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};

const DEFAULT_CGROUP_NAME: &str = "neon-postgres";

/// Flag for whether to run postgres in a cgroup
#[derive(Debug, Clone)]
pub enum CgroupMode {
    /// Run it in the cgroup with name `name`
    ///
    /// Beware: things may behave incorrectly if `name` contains path separators. Refer to the
    /// 'FIXME' comments in the rest of this file for more details.
    Enabled {
        name: String,
    },
    Disabled,
}

impl CgroupMode {
    /// Returns the `CgroupMode` with the default cgroup name enabled
    pub fn default_enabled() -> Self {
        CgroupMode::Enabled {
            name: DEFAULT_CGROUP_NAME.to_owned(),
        }
    }

    /// If `Cgroup::Enabled`, ensures that the cgroup exists and has the necessary controllers
    /// enabled
    pub fn setup(&self) -> Result<()> {
        let name = match self {
            CgroupMode::Disabled => return Ok(()),
            CgroupMode::Enabled { name } => name,
        };

        let cgroup_path = Path::new("/sys/fs/cgroup/").join(name.as_str());

        // Enable the controllers separately, so that we can have better errors if it fails.
        //
        // Refer to `man 7 cgroups`, e.g. <https://man7.org/linux/man-pages/man7/cgroups.7.html>,
        // specifically the section about "cgroup.subtree_control".
        //
        // In short, writing `+$controller` (with $controller as a stand-in for the name of the
        // controller) to `$cgroup/cgroup.subtree_control` will enable the controller in all
        // its child cgroups.
        //
        // Note: At time of writing (NeonVM v0.4.4), VMs created with NeonVM's vm-builder don't
        // have the typical set of controllers enabled by default. This is necessary because things
        // like memory and cpu are not enabled by default; we have to enable them ourselves.
        //
        // FIXME: This won't *really* work if the cgroup name includes slashes, because maybe its
        // parent cgroup already exists. An ideal version of this would traverse down the path to
        // the cgroup, enabling all of each parent's controllers in the children. That's a lot of
        // work for something that's not a cgroups library.
        //
        // See also:
        //   https://github.com/neondatabase/autoscaling/blob/ae28c8ab6f4c1028a72f0ea8a8b12557003b0d97/vm_image/init#L10-L16
        const CONTROLLERS: &[&str] = &["cpu", "memory"];

        let subtree_control_path = "/sys/fs/cgroup/cgroup.subtree_control";
        for c in CONTROLLERS {
            let content = format!("+{c}");
            fs::write(subtree_control_path, content).with_context(|| {
                format!("could not enable {c:?} controller at path {subtree_control_path:?}")
            })?;
        }

        // Create the cgroup (or do nothing, if it already exists)
        fs::create_dir_all(&cgroup_path)
            .with_context(|| format!("could not create_dir_all({cgroup_path:?})"))
    }

    /// If `Cgroup::Enabled`, makes sure that the cgroup is not frozen
    pub fn ensure_thawed(&self) -> Result<()> {
        match self {
            CgroupMode::Disabled => Ok(()),
            CgroupMode::Enabled { name } => {
                let path = Path::new("/sys/fs/cgroup/")
                    .join(name)
                    .join("cgroup.freeze");
                // Refer to: <https://docs.kernel.org/admin-guide/cgroup-v2.html>, under the
                // section "cgroup.freeze".
                fs::write(path, "0").with_context(|| format!("could not "))
            }
        }
    }

    /// Returns a new `Command`, wrapped in `cgexec` if `CgroupMode::Enabled`
    ///
    /// In short: Returns `Command::new(program)` if the cgroup isn't enabled; otherwise returns a
    /// `Command` that will run `program` in the configured cgroup.
    pub fn command<S: AsRef<OsStr>>(&self, program: S) -> Command {
        match self {
            CgroupMode::Disabled => Command::new(program),
            CgroupMode::Enabled { name } => {
                let mut cmd = Command::new("cgexec");
                // Force all child subprocesses to remain in the cgroup.
                cmd.arg("--sticky");
                // -g *:$name runs the command with all controllers for the cgroup $name.
                // For more, refer to `man 1 cgexec`, e.g. <https://linux.die.net/man/1/cgexec>
                cmd.arg("-g").arg(format!("*:{name}"));
                cmd
            }
        }
    }
}
