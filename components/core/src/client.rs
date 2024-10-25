/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
pub trait Client {
    type Error;
    fn connect(self) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send
    where
        Self: Sized;
}
