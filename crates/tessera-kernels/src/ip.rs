// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Matching addresses against the patterns of `LogQL`'s `ip()` label
//! filter (QL-7): one address, a CIDR block such as `10.0.0.0/8`, or
//! an inclusive range such as `10.0.0.1-10.0.0.9`, over IPv4 and IPv6.
//! A value that is not an address never matches.

use std::net::IpAddr;

/// A parsed `ip()` pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Matcher {
    /// One address.
    One(IpAddr),
    /// A CIDR block: the addresses sharing the first `prefix` bits.
    Block {
        /// The network address.
        network: IpAddr,
        /// How many leading bits must agree.
        prefix: u8,
    },
    /// Every address from `start` through `end`, in one family.
    Range {
        /// The first address.
        start: IpAddr,
        /// The last address.
        end: IpAddr,
    },
}

impl Matcher {
    /// The matcher `pattern` describes, or `None` when it is none of
    /// the three forms.
    #[must_use]
    pub fn parse(pattern: &str) -> Option<Matcher> {
        let pattern = pattern.trim();
        if let Some((network, prefix)) = pattern.split_once('/') {
            let network: IpAddr = network.parse().ok()?;
            let prefix: u8 = prefix.parse().ok()?;
            let width = if network.is_ipv4() { 32 } else { 128 };
            return (prefix <= width).then_some(Matcher::Block { network, prefix });
        }
        if let Some((start, end)) = pattern.split_once('-') {
            let (start, end): (IpAddr, IpAddr) = (start.parse().ok()?, end.parse().ok()?);
            return (start.is_ipv4() == end.is_ipv4() && bits(start) <= bits(end))
                .then_some(Matcher::Range { start, end });
        }
        pattern.parse().ok().map(Matcher::One)
    }

    /// Whether `address` is one the pattern names.
    #[must_use]
    pub fn matches(&self, address: &str) -> bool {
        let Ok(address) = address.trim().parse::<IpAddr>() else {
            return false;
        };
        match self {
            Matcher::One(one) => *one == address,
            Matcher::Block { network, prefix } => {
                if network.is_ipv4() != address.is_ipv4() {
                    return false;
                }
                let width = if network.is_ipv4() { 32 } else { 128 };
                let shift = u32::from(width - prefix);
                let keep = |ip: IpAddr| if shift >= 128 { 0 } else { bits(ip) >> shift };
                keep(*network) == keep(address)
            }
            Matcher::Range { start, end } => {
                start.is_ipv4() == address.is_ipv4()
                    && bits(*start) <= bits(address)
                    && bits(address) <= bits(*end)
            }
        }
    }
}

/// The address as an integer, IPv4 in the low 32 bits.
fn bits(address: IpAddr) -> u128 {
    match address {
        IpAddr::V4(v4) => u128::from(u32::from(v4)),
        IpAddr::V6(v6) => u128::from(v6),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_address_matches_itself_alone() {
        let one = Matcher::parse("10.0.0.5").unwrap();
        assert!(one.matches("10.0.0.5"));
        assert!(one.matches(" 10.0.0.5 "));
        assert!(!one.matches("10.0.0.6"));
        assert!(!one.matches("not an address"));
    }

    #[test]
    fn a_block_matches_its_prefix_in_either_family() {
        let v4 = Matcher::parse("192.168.0.0/16").unwrap();
        assert!(v4.matches("192.168.7.9"));
        assert!(!v4.matches("192.169.0.1"));
        assert!(!v4.matches("::ffff:192.168.0.1"), "another family");
        let v6 = Matcher::parse("2001:db8::/32").unwrap();
        assert!(v6.matches("2001:db8:1::1"));
        assert!(!v6.matches("2001:db9::1"));
        assert!(Matcher::parse("0.0.0.0/0").unwrap().matches("8.8.8.8"));
        assert_eq!(Matcher::parse("10.0.0.0/33"), None);
    }

    #[test]
    fn a_range_is_inclusive_at_both_ends() {
        let range = Matcher::parse("10.0.0.1-10.0.0.9").unwrap();
        assert!(range.matches("10.0.0.1") && range.matches("10.0.0.9"));
        assert!(range.matches("10.0.0.4"));
        assert!(!range.matches("10.0.0.10"));
        assert_eq!(Matcher::parse("10.0.0.9-10.0.0.1"), None, "backwards");
        assert_eq!(Matcher::parse("10.0.0.1-::1"), None, "mixed families");
        assert_eq!(Matcher::parse("hosts"), None);
    }
}
