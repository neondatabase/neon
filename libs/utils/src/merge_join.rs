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

pub fn left_equi_join_with_merge_strategy<L, LI, R, RI, K, FL, FR>(
    l: L,
    r: R,
    key_l: FL,
    key_r: FR,
) -> impl Iterator<Item = (LI, Option<RI>)>
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
    let mut l_had_match = false;
    std::iter::from_fn(move || {
        loop {
            match (l.peek(), r.peek()) {
                (Some((_, lk)), Some((_, rk))) if lk < rk => {
                    let (lv, _) = l.next().unwrap();
                    if l_had_match {
                        l_had_match = false;
                        continue;
                    } else {
                        return Some((lv, None));
                    }
                }
                (Some((_, _)), None) => {
                    let (lv, _) = l.next().unwrap();
                    if l_had_match {
                        l_had_match = false;
                        continue;
                    } else {
                        return Some((lv, None));
                    }
                }
                (Some((_, lk)), Some((_, rk))) if lk > rk => {
                    drop(r.next());
                    continue;
                }
                (Some((lv, lk)), Some((_, rk))) => {
                    l_had_match = true;
                    assert!(lk == rk);
                    let (rv, _) = r.next().unwrap();
                    return Some((lv.clone(), Some(rv)));
                }
                (None, None) | (None, Some(_)) => return None,
            }
        }
    })
}
#[cfg(test)]
mod tests {

    #[test]
    fn inner_equi_basic() {
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

    #[test]
    fn left_equi_basic() {
        /*
        create table aleft (id text, aleft text);
        create table aright (id text, aright text);
        insert into aleft values ('a', 'a'), ('b', 'b');
        insert into aright values ('a', 'aa'), ('a', 'ab'), ('c', 'cd');
        select * from aleft left join aright using ("id");
        */

        let l = vec![b"a", b"b"];
        let r = vec![b"aa", b"ab", b"cd"];

        let res: Vec<_> = super::left_equi_join_with_merge_strategy(
            l.into_iter(),
            r.into_iter(),
            |l| &l[0..1],
            |r| &r[0..1],
        )
        .collect();

        assert_eq!(
            res,
            vec![(b"a", Some(b"aa")), (b"a", Some(b"ab")), (b"b", None)]
        );
    }

    #[test]
    fn left_equi_basic_2() {
        let l = vec![b"b"];
        let r = vec![b"aa", b"ab", b"bb"];

        let res: Vec<_> = super::left_equi_join_with_merge_strategy(
            l.into_iter(),
            r.into_iter(),
            |l| &l[0..1],
            |r| &r[0..1],
        )
        .collect();

        assert_eq!(res, vec![(b"b", Some(b"bb"))])
    }
}
