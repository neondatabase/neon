#![no_main]
use libfuzzer_sys::fuzz_target;

// fuzz_target!(|data: &[u8]| {
//     safekeeper::safekeeper::fuzz(data)
// });

fuzz_target!(|messages: Vec<safekeeper::safekeeper::ProposerAcceptorMessage>| {
    safekeeper::safekeeper::fuzz_messages(messages)
});
