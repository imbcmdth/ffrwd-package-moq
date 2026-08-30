//! Conversions between `std::net::SocketAddr` and wasi socket addresses.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use wasi::sockets::network::{IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress};

pub fn to_wasi(addr: SocketAddr) -> IpSocketAddress {
    match addr {
        SocketAddr::V4(v4) => {
            let [a, b, c, d] = v4.ip().octets();
            IpSocketAddress::Ipv4(Ipv4SocketAddress {
                port: v4.port(),
                address: (a, b, c, d),
            })
        }
        SocketAddr::V6(v6) => {
            let [a, b, c, d, e, f, g, h] = v6.ip().segments();
            IpSocketAddress::Ipv6(Ipv6SocketAddress {
                port: v6.port(),
                flow_info: v6.flowinfo(),
                address: (a, b, c, d, e, f, g, h),
                scope_id: v6.scope_id(),
            })
        }
    }
}

pub fn from_wasi(addr: IpSocketAddress) -> SocketAddr {
    match addr {
        IpSocketAddress::Ipv4(v4) => {
            let (a, b, c, d) = v4.address;
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(a, b, c, d), v4.port))
        }
        IpSocketAddress::Ipv6(v6) => {
            let (a, b, c, d, e, f, g, h) = v6.address;
            SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::new(a, b, c, d, e, f, g, h),
                v6.port,
                v6.flow_info,
                v6.scope_id,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v4_round_trips() {
        let addr: SocketAddr = "127.0.0.1:4433".parse().unwrap();
        assert_eq!(from_wasi(to_wasi(addr)), addr);
    }

    #[test]
    fn v6_round_trips_with_flow_and_scope() {
        let addr = SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 443, 7, 3));
        assert_eq!(from_wasi(to_wasi(addr)), addr);
    }
}
