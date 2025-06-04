pub fn inner_equi_join_with_merge_strategy<L, LI, R, RI, K, FL, FR>(
    l: L,
    r: R,
    key_l: FL,
    key_r: FR,
) -> impl Iterator<Item = (LI, RI)>
where
    L: Iterator<Item = LI>, // + Sorted
    R: Iterator<Item = RI>, // + Sorted
    FL: 'static + Fn(&LI) -> K,
    FR: 'static + Fn(&RI) -> K,
    LI: Copy,
    RI: Copy,
    K: PartialEq + Eq + Ord,
{
    let mut l = l.map(move |i| (i, key_l(&i))).peekable();
    let mut r = r.map(move |i| (i, key_r(&i))).peekable();
    std::iter::from_fn(move || {
        loop {
            match (l.peek(), r.peek()) {
                (Some((_, lk)), Some((_, rk))) if lk < rk => {
                    drop(l.next());
                    continue;
                }
                (Some((_, lk)), Some((_, rk))) if lk > rk => {
                    drop(r.next());
                    continue;
                }
                (Some((lv, lk)), Some((_, rk))) => {
                    assert!(lk == rk);
                    let (rv, _) = r.next().unwrap();
                    return Some((lv.clone(), rv));
                }
                (None, None) | (None, Some(_)) | (Some(_), None) => return None,
            }
        }
    })
}

#[cfg(test)]
mod tests {

    #[test]
    fn basic() {
        let l = vec![b"a", b"c"];
        let r = vec![b"aa", b"ad", b"ba", b"bb", b"ca", b"cb", b"cd", b"dd"];

        let res: Vec<_> = super::inner_equi_join_with_merge_strategy(
            l.into_iter(),
            r.into_iter(),
            |l| &l[0..1],
            |r| &r[0..1],
        )
        .collect();

        assert_eq!(
            res,
            vec![
                (b"a", b"aa"),
                (b"a", b"ad"),
                (b"c", b"ca"),
                (b"c", b"cb"),
                (b"c", b"cd"),
            ]
        );
    }
}
