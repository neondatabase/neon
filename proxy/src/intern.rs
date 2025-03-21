use core::fmt;

use paracord::custom_key;

use crate::types::{AccountId, BranchId, EndpointId, ProjectId, RoleName};

custom_key!(pub struct RoleNameInt);
custom_key!(pub struct EndpointIdInt);
custom_key!(pub struct BranchIdInt);
custom_key!(pub struct ProjectIdInt);
custom_key!(pub struct AccountIdInt);

impl From<&RoleName> for RoleNameInt {
    fn from(value: &RoleName) -> Self {
        RoleNameInt::get_or_intern(value)
    }
}
impl fmt::Display for RoleNameInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.resolve())
    }
}

impl From<&EndpointId> for EndpointIdInt {
    fn from(value: &EndpointId) -> Self {
        EndpointIdInt::get_or_intern(value)
    }
}
impl From<EndpointId> for EndpointIdInt {
    fn from(value: EndpointId) -> Self {
        EndpointIdInt::get_or_intern(&value)
    }
}

impl From<&BranchId> for BranchIdInt {
    fn from(value: &BranchId) -> Self {
        BranchIdInt::get_or_intern(value)
    }
}
impl From<BranchId> for BranchIdInt {
    fn from(value: BranchId) -> Self {
        BranchIdInt::get_or_intern(&value)
    }
}
impl AsRef<str> for BranchIdInt {
    fn as_ref(&self) -> &str {
        self.resolve()
    }
}
impl std::ops::Deref for BranchIdInt {
    type Target = str;
    fn deref(&self) -> &str {
        self.resolve()
    }
}

impl From<&ProjectId> for ProjectIdInt {
    fn from(value: &ProjectId) -> Self {
        ProjectIdInt::get_or_intern(value)
    }
}
impl From<ProjectId> for ProjectIdInt {
    fn from(value: ProjectId) -> Self {
        ProjectIdInt::get_or_intern(&value)
    }
}
impl AsRef<str> for ProjectIdInt {
    fn as_ref(&self) -> &str {
        self.resolve()
    }
}
impl std::ops::Deref for ProjectIdInt {
    type Target = str;
    fn deref(&self) -> &str {
        self.resolve()
    }
}
impl fmt::Display for ProjectIdInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.resolve())
    }
}

impl From<&AccountId> for AccountIdInt {
    fn from(value: &AccountId) -> Self {
        AccountIdInt::get_or_intern(value)
    }
}
impl From<AccountId> for AccountIdInt {
    fn from(value: AccountId) -> Self {
        AccountIdInt::get_or_intern(&value)
    }
}
impl fmt::Display for AccountIdInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.resolve())
    }
}
