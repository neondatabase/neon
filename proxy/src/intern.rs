use paracord::custom_key;

use crate::types::{AccountId, BranchId, EndpointId, ProjectId, RoleName};

custom_key!(pub struct RoleNameInt);
custom_key!(pub struct EndpointIdInt);
custom_key!(pub struct BranchIdInt);
custom_key!(pub struct ProjectIdInt);
custom_key!(pub struct AccountIdInt);

impl From<&RoleName> for RoleNameInt {
    fn from(value: &RoleName) -> Self {
        RoleNameInt::new(value)
    }
}

impl From<&EndpointId> for EndpointIdInt {
    fn from(value: &EndpointId) -> Self {
        EndpointIdInt::new(value)
    }
}
impl From<EndpointId> for EndpointIdInt {
    fn from(value: EndpointId) -> Self {
        EndpointIdInt::new(&value)
    }
}

impl From<&BranchId> for BranchIdInt {
    fn from(value: &BranchId) -> Self {
        BranchIdInt::new(value)
    }
}
impl From<BranchId> for BranchIdInt {
    fn from(value: BranchId) -> Self {
        BranchIdInt::new(&value)
    }
}

impl From<&ProjectId> for ProjectIdInt {
    fn from(value: &ProjectId) -> Self {
        ProjectIdInt::new(value)
    }
}
impl From<ProjectId> for ProjectIdInt {
    fn from(value: ProjectId) -> Self {
        ProjectIdInt::new(&value)
    }
}

impl From<&AccountId> for AccountIdInt {
    fn from(value: &AccountId) -> Self {
        AccountIdInt::new(value)
    }
}
impl From<AccountId> for AccountIdInt {
    fn from(value: AccountId) -> Self {
        AccountIdInt::new(&value)
    }
}
