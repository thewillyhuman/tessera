// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `PromQL` front end (spec section 9.3): a hand-written lexer and
//! parser with positioned errors (QL-2). Lowering into the shared plan
//! arrives with its own commit.

pub mod ast;
pub mod lexer;
pub mod parser;

pub use parser::parse;
