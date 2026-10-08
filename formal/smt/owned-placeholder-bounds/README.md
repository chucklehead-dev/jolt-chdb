# Private owned placeholder read bounds

This is a bounded numeric/access model, not a proof of SQL classification,
lexical correctness, compiler correctness, or Durable publication/recovery.

Source mapping: `owned_statement.clj` admits sealed private ASCII snapshots
up to 64MiB; `owned_ascii_placeholder.ss` starts index zero and checks `i<n`
before reading the current byte. Lookahead is read only at `i+1<n`. A native
32-bit word is read only with `i mod4=0` and `i+4<=n` on the 64-bit target.
The available steps are one, two (only with a real next byte), or four (only
for a complete aligned word). The arithmetic remains unchanged.

`access.smt2` asks for an admitted read whose start is negative or whose end
exceeds the backing length. Chiasmus returned UNSAT. `induction.smt2` asks
for a successor outside `[0,n]` or an overflowing `i+4`; it returned UNSAT.
The model bounds length to 0..67108864 and assumes the supported sealed
snapshot cannot be mutated through private reflection or unsafe FFI. The
byte-array ABI, immutability, and fixnum representation are trusted premises.

Controls use the same read-index/width/violation schema:

- `lookahead-mutant`: replace `<` with `<=`; SAT, n1/i0/start1/width1.
- `small-control`: first byte of a length-one array; UNSAT violation query.
- `inclusive-word`: length4/index0/width4; SAT with violation false.

The runtime mutant must first replace ALL specialized accesses with checked
accesses. Its out-of-range witness then throws safely. Runtime tail/quote/
comment tests compare results with the portable detector. Existing ownership,
all-ASCII, WAL-parity and real-native snapshot gates remain separate obligations.

Word matching, mode transitions, nesting depth and the bit-mask's conservative
false-positive behavior are not encoded here; they are unchanged by this
read-only specialization and require the lexical oracle tests. No public array
access, SQL parser, classification decision, global optimization level or
persisted format is changed.
