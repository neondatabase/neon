use paracord::custom_key;

use crate::types::{AccountId, BranchId, EndpointId, ProjectId, RoleName};

custom_key!(pub struct RoleNameInt);
custom_key!(pub struct EndpointIdInt);
custom_key!(pub struct BranchIdInt);
custom_key!(pub struct ProjectIdInt);
custom_key!(pub struct AccountIdInt);

impl From<&RoleName> for RoleNameInt {
    fn from(value: &RoleName) -> Self {
        RoleNameInt::from_str_or_intern(value)
    }
}

impl From<&EndpointId> for EndpointIdInt {
    fn from(value: &EndpointId) -> Self {
        EndpointIdInt::from_str_or_intern(value)
    }
}
impl From<EndpointId> for EndpointIdInt {
    fn from(value: EndpointId) -> Self {
        EndpointIdInt::from_str_or_intern(&value)
    }
}

impl From<&BranchId> for BranchIdInt {
    fn from(value: &BranchId) -> Self {
        BranchIdInt::from_str_or_intern(value)
    }
}
impl From<BranchId> for BranchIdInt {
    fn from(value: BranchId) -> Self {
        BranchIdInt::from_str_or_intern(&value)
    }
}
impl std::ops::Deref for BranchIdInt {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl From<&ProjectId> for ProjectIdInt {
    fn from(value: &ProjectId) -> Self {
        ProjectIdInt::from_str_or_intern(value)
    }
}
impl From<ProjectId> for ProjectIdInt {
    fn from(value: ProjectId) -> Self {
        ProjectIdInt::from_str_or_intern(&value)
    }
}
impl std::ops::Deref for ProjectIdInt {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl From<&AccountId> for AccountIdInt {
    fn from(value: &AccountId) -> Self {
        AccountIdInt::from_str_or_intern(value)
    }
}
impl From<AccountId> for AccountIdInt {
    fn from(value: AccountId) -> Self {
        AccountIdInt::from_str_or_intern(&value)
    }
}
