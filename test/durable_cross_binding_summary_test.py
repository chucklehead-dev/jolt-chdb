#!/usr/bin/env python3
import copy, hashlib, json, os, pathlib, subprocess, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SUMMARY = ROOT / "scripts/summarize-durable-cross-binding-recovery.py"
PREPARE = ROOT / "scripts/prepare-durable-cross-binding-run.py"
PROFILE = ROOT / "scripts/profile-durable-cross-binding-recovery.sh"
SHELL_LIB = ROOT / "scripts/durable-cross-binding-recovery-lib.sh"
def canonical(v): return json.dumps(v, sort_keys=True, separators=(",", ":")).encode()
def sha(v): return hashlib.sha256(v).hexdigest()
def schedule(trials):
    out=[]
    def add(phase,runtime,trial,label): out.append({"ordinal":len(out),"phase":phase,"runtime":runtime,"trial":trial,"report_file":f"{runtime}-{label}.json","time_file":f"{runtime}-{label}.time"})
    add("prime","rust",None,"prime"); add("prime","jolt",None,"prime")
    for trial in range(1,trials+1):
        for runtime in (("rust","jolt") if trial%2 else ("jolt","rust")): add("measured",runtime,trial,f"trial-{trial}")
    return out

class SummaryTest(unittest.TestCase):
    def write(self,path,value): path.write_text(json.dumps(value))
    def corpus(self, root, trials=5):
        reports=root/"reports"; reports.mkdir(); (reports/"harness-tracked.patch").write_bytes(b"")
        state={"schema_version":1,"head":"1"*40+"a"*24,"parent":"2"*64,"tree":"3"*64,"status":"clean","tracked_patch":{"file_name":"harness-tracked.patch","bytes":0,"sha256":sha(b"")},"untracked_files":[]}
        state["state_sha256"]=sha(canonical(state)); self.write(reports/"harness-state.json",state)
        config={"object_id":"cross-binding-recovery","database":"benchmark","trials":trials,"batch_size":512,"warmup_batches":2,"measured_batches":8,"total_rows":5120}
        run={"schema_version":1,"config":config,"harness_state":state,"schedule":schedule(trials)}; run["run_id"]=sha(canonical(run)); self.write(reports/"run-manifest.json",run)
        inventory=[{"key":".head-versions/1.json","kind":"file","bytes":1,"sha256":"9"*64,"target":None},{"key":"head.json","kind":"symlink","bytes":None,"sha256":None,"target":".head-versions/1.json"}]; wal=[]
        for n in range(1,4):
            key=f"wal/x-{n}-id.jsonl"; ident={"key":key,"size":n,"sha256":str(n)*64}; wal.append(ident); inventory.append({"key":key,"kind":"file","bytes":n,"sha256":str(n)*64,"target":None})
        expected={"n":"5120","flags":"2560","severity_sum":"1","body_bytes":"1","question_bodies":"0","min_trace":"a","max_trace":"z","min_span":"a","max_span":"z"}
        native={"file_name":"libchdb.so","bytes":10,"sha256":"a"*64}; header={"file_name":"chdb.h","bytes":10,"sha256":"b"*64}
        rust={"runtime":"rust","chdb_rust_git_sha":"c"*64,"chdb_rust_crate_version":"1.4.0","oracle_crate_version":"0.1.0","rustc_version":"rustc 1","cargo_version":"cargo 1","engine_source":"bundled","native_version":"26.7.2","harness_state":state,"native_library":native,"native_header":header,"executable":{"file_name":"oracle","bytes":10,"sha256":"d"*64}}
        fixture={"schema_version":1,"run_id":run["run_id"],"producer":rust,"config":{k:config[k] for k in ("object_id","database","batch_size","warmup_batches","measured_batches","total_rows")},"expected":expected,"manifest":{"db":"benchmark","base":None,"wal":wal,"seq":3},"inventory":inventory,"inventory_sha256":sha(json.dumps(inventory,separators=(",", ":")).encode())}; self.write(reports/"fixture.json",fixture)
        describe='{:version "v0.8.6-1-geeeeeeee"\n :project-dir "."\n :config-files ["./deps.edn"]\n :config-user nil\n :config-project "./deps.edn"\n :gitlibs-dir "/tmp/oracle/jolt-gitlibs"\n :mvn-local-repo "/tmp/m2"\n :repro true\n :aliases []}\n'
        (reports/"jolt-sdescribe.edn").write_text(describe)
        describe_identity={"file_name":"jolt-sdescribe.edn","bytes":len(describe.encode()),"sha256":sha(describe.encode())}
        jolt={"runtime":"jolt","jolt_version":"jolt v0.8.6-1-geeeeeeee","jolt_source_sha_asserted":"e"*40,"jolt_executable_revision":"e"*8,"jolt_sdescribe":describe_identity,"jolt_config_mode":"Srepro-project-only","jolt_cache_scope":"run-scoped-isolated-after-prime","scheme_version":"10.4.1","machine_type":"ta6le","native_version":"26.7.2","harness_state":state,"native_library":native,"native_header":header,"executable":{"file_name":"jolt","bytes":20,"sha256":"f"*64}}
        for entry in run["schedule"]:
            provenance=rust if entry["runtime"]=="rust" else jolt; elapsed=1_000_000_000 if entry["runtime"]=="rust" else 1_200_000_000; ordinal=entry["ordinal"]
            report={"schema_version":1,"run_id":run["run_id"],"schedule_ordinal":ordinal,"phase":entry["phase"],"runtime":provenance,"trial":entry["trial"],"process_id":1000+ordinal,"process_started_epoch_ms":2000+2*ordinal,"process_finished_epoch_ms":2001+2*ordinal,"cache_condition":"warm-provider-cache-fresh-process-engine-and-scratch","fixture":{"inventory_sha256":fixture["inventory_sha256"],"batch_size":512,"warmup_batches":2,"measured_batches":8,"wal_segments":3,"recovered_rows":5120},"recovery":{"elapsed_ns":elapsed,"rows_per_second":5120e9/elapsed,"expected":expected,"actual":expected,"inventory_unchanged":True}}
            self.write(reports/entry["report_file"],report); (reports/entry["time_file"]).write_text("Elapsed (wall clock) time (h:mm:ss or m:ss): 0:01.50\nMaximum resident set size (kbytes): 12345\nExit status: 0\n")
        return reports
    def run_summary(self,reports):
        output=reports/"summary.json"; result=subprocess.run(["python3",str(SUMMARY),str(reports),str(output)],text=True,capture_output=True); return result,output
    def test_happy_path_has_in_region_and_whole_process_ratios(self):
        with tempfile.TemporaryDirectory() as tmp:
            result,out=self.run_summary(self.corpus(pathlib.Path(tmp),trials=7)); self.assertEqual(0,result.returncode,result.stderr); comparison=json.loads(out.read_text())["comparison"]
            self.assertAlmostEqual(1.2,comparison["acceptance"]["jolt_elapsed_over_rust"]); self.assertAlmostEqual(1/1.2,comparison["acceptance"]["jolt_throughput_over_rust"]); self.assertAlmostEqual(1,comparison["whole_process_diagnostic"]["jolt_elapsed_over_rust"])
            self.assertIn("n=7",json.loads(out.read_text())["semantics"]["percentiles"])
    def mutate_json(self,reports,name,mutation):
        path=reports/name; value=json.loads(path.read_text()); mutation(value); self.write(path,value)
    def test_causal_mutations_are_rejected(self):
        cases=[
          ("fixture.json",lambda x:x["producer"].update(native_version=None),"native version"),
          ("fixture.json",lambda x:x["expected"].update(n="5119"),"expected count"),
          ("fixture.json",lambda x:x.update(unknown=True),"unknown fields"),
          ("jolt-trial-1.json",lambda x:x["runtime"].update(jolt_source_sha_asserted="short"),"Jolt SHA"),
          ("rust-trial-1.json",lambda x:x["runtime"].pop("cargo_version"),"cargo_version is missing"),
          ("rust-trial-2.json",lambda x:x["runtime"].update(oracle_crate_version="changed"),"Rust provenance differs"),
          ("rust-trial-3.json",lambda x:x["runtime"].update(engine_source="changed"),"Rust provenance differs"),
          ("jolt-trial-2.json",lambda x:x["runtime"]["native_header"].update(sha256="9"*64),"provenance differs"),
          ("jolt-trial-1.json",lambda x:x["runtime"].update(scheme_version="changed"),"Jolt provenance differs"),
          ("jolt-trial-2.json",lambda x:x["runtime"].update(machine_type="changed"),"Jolt provenance differs"),
          ("jolt-trial-3.json",lambda x:x.update(process_id=1000),"not fresh"),
          ("jolt-trial-4.json",lambda x:x["recovery"].update(extra=True),"unknown fields"),
          ("rust-trial-4.json",lambda x:x["recovery"].update(rows_per_second=1.0),"does not recompute"),
        ]
        for name,mutation,message in cases:
            with self.subTest(name=name,message=message), tempfile.TemporaryDirectory() as tmp:
                reports=self.corpus(pathlib.Path(tmp)); self.mutate_json(reports,name,mutation); result,_=self.run_summary(reports); self.assertNotEqual(0,result.returncode); self.assertIn(message,result.stderr)
    def test_rejects_missing_prime_and_extra_numbered_time(self):
        for mutate,message in ((lambda r:(r/"rust-prime.json").unlink(),"cannot read rust-prime.json"),(lambda r:(r/"rust-trial-99.time").write_text("x"),"file set is not exact")):
            with self.subTest(message=message), tempfile.TemporaryDirectory() as tmp:
                reports=self.corpus(pathlib.Path(tmp)); mutate(reports); result,_=self.run_summary(reports); self.assertNotEqual(0,result.returncode); self.assertIn(message,result.stderr)
    def test_rejects_one_trial_corpus(self):
        with tempfile.TemporaryDirectory() as tmp:
            result,_=self.run_summary(self.corpus(pathlib.Path(tmp),trials=1)); self.assertNotEqual(0,result.returncode); self.assertIn("at least five measured trials",result.stderr)
            prepared=subprocess.run(["python3",str(PREPARE),str(ROOT),str(pathlib.Path(tmp)/"new"),"object","1","512","2","8"],text=True,capture_output=True)
            self.assertNotEqual(0,prepared.returncode); self.assertIn("at least five measured trials",prepared.stderr)
            profiled=subprocess.run(["bash",str(PROFILE),str(pathlib.Path(tmp)/"profile"),"missing-jolt","f"*40,"missing-lib", "1"],text=True,capture_output=True)
            self.assertEqual(2,profiled.returncode); self.assertIn("at least five measured trials",profiled.stderr)
    def test_rejects_nonzero_time_and_schedule_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports=self.corpus(pathlib.Path(tmp)); (reports/"rust-trial-1.time").write_text("Elapsed (wall clock) time (h:mm:ss or m:ss): 0:01\nMaximum resident set size (kbytes): 1\nExit status: 1\n"); result,_=self.run_summary(reports); self.assertIn("incomplete/failed",result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            reports=self.corpus(pathlib.Path(tmp)); self.mutate_json(reports,"run-manifest.json",lambda x:x["schedule"].reverse()); result,_=self.run_summary(reports); self.assertIn("schedule is not exact",result.stderr)

    def test_rejects_unbound_source_and_changed_sdescribe(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports=self.corpus(pathlib.Path(tmp))
            for path in reports.glob("jolt-*.json"):
                self.mutate_json(reports,path.name,lambda x:x["runtime"].update(jolt_source_sha_asserted="a"*40))
            result,_=self.run_summary(reports)
            self.assertNotEqual(0,result.returncode)
            self.assertIn("does not agree",result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            reports=self.corpus(pathlib.Path(tmp))
            (reports/"jolt-sdescribe.edn").write_text("changed")
            result,_=self.run_summary(reports)
            self.assertNotEqual(0,result.returncode)
            self.assertIn("exact repro project configuration",result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            reports=self.corpus(pathlib.Path(tmp))
            for path in reports.glob("jolt-*.json"):
                self.mutate_json(reports,path.name,lambda x:x["runtime"].update(jolt_version="v0.8.6-1-geeeeeeee"))
            result,_=self.run_summary(reports)
            self.assertNotEqual(0,result.returncode)
            self.assertIn("actual jolt --version banner",result.stderr)

    def test_same_banner_prefix_does_not_overclaim_full_source_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports=self.corpus(pathlib.Path(tmp))
            same_prefix=("e"*8)+("a"*32)
            for path in reports.glob("jolt-*.json"):
                self.mutate_json(reports,path.name,lambda x:x["runtime"].update(jolt_source_sha_asserted=same_prefix))
            result,_=self.run_summary(reports)
            self.assertEqual(0,result.returncode,result.stderr)

    def test_acceptance_region_reconciles_before_elapsed_capture(self):
        jolt=(ROOT/"bench/jdbc/chdb_durable_cross_binding_recovery.clj").read_text()
        rust=(ROOT/"bench/rust-durable-recovery-oracle/src/main.rs").read_text()
        self.assertLess(jolt.index("(when-not (= expected actual)"),
                        jolt.index("elapsed (- (System/nanoTime) start)"))
        self.assertLess(rust.index("if actual != descriptor.expected"),
                        rust.index("let elapsed_ns = start.elapsed()"))

    def shell_contract_trace(self, library):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp); lib=root/"lib.sh"; trace=root/"trace"
            lib.write_text(library)
            timer=root/"time"; timer.write_text('#!/usr/bin/env bash\nset -eu\nshift\n[[ "$1" == "-o" ]]\nshift\ntime_file=$1\nshift\n: > "$time_file"\nexec "$@"\n'); timer.chmod(0o755)
            executable=root/"runtime"; executable.write_text('#!/usr/bin/env bash\nprintf "%s|%s|%s|%s\\n" "${ROLE:-rust}" "${JOLT_CACHE_DIR:-}" "${JOLT_GITLIBS_DIR:-}" "$*" >> "$TRACE"\n'); executable.chmod(0o755)
            script='''set -euo pipefail
source "$1"
verify_harness_state() { printf 'verify\\n' >> "$TRACE"; }
report_dir="$2/reports"; mkdir -p "$report_dir"; time_bin="$3"; wrapper="$4"; rust_bin="$4"
native_dir="$2/native"; libchdb="$2/native/libchdb.so"; native_header="$2/native/chdb.h"
rustc_version=rustc; cargo_version=cargo; harness_state="$2/state"; fixture_root="$2/fixture"
object_id=object; descriptor="$2/descriptor"; run_manifest="$2/manifest"
jolt_cache="$2/cache"; jolt_gitlibs="$2/gitlibs"; jolt_bin="$2/jolt"
jolt_source_sha_asserted=eeeeeeeeaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
jolt_executable_revision=eeeeeeee; jolt_version=jolt-v; jolt_describe="$2/describe"
ROLE=jolt run_jolt trial-1 "$2/jolt.json" 2
ROLE=rust run_rust trial-1 "$2/rust.json" 3
'''
            env={**os.environ,"TRACE":str(trace)}
            result=subprocess.run(["bash","-c",script,"bash",str(lib),str(root),str(timer),str(executable)],text=True,capture_output=True,env=env)
            self.assertEqual(0,result.returncode,result.stderr)
            lines=trace.read_text().splitlines()
            self.assertEqual("verify",lines[0]); self.assertEqual("verify",lines[2])
            self.assertTrue(lines[1].startswith(f"jolt|{root}/cache|{root}/gitlibs|"),lines[1])
            self.assertIn(" -Srepro -M:durable-cross-binding-recovery ",f" {lines[1]} ")
            self.assertTrue(lines[3].startswith("rust|||recover "),lines[3])

    def test_shell_runtime_contract_is_mutation_sensitive(self):
        library=SHELL_LIB.read_text(); self.shell_contract_trace(library)
        mutations=[
            ('"$jolt_bin" -Srepro -M:durable-cross-binding-recovery','"$jolt_bin" -M:durable-cross-binding-recovery'),
            ('    env JOLT_CACHE_DIR="$jolt_cache" \\\n','    env \\\n'),
            ('        JOLT_GITLIBS_DIR="$jolt_gitlibs" \\\n','        \\\n'),
            ('run_jolt() {','run_jolt() {')]
        jolt_start=library.index("run_jolt() {")
        verify_at=library.index("  verify_harness_state",jolt_start)
        mutations[-1]=(library[verify_at:verify_at+len("  verify_harness_state")],"  :")
        for old,new in mutations:
            with self.subTest(mutation=old):
                mutated=library.replace(old,new,1)
                self.assertNotEqual(library,mutated)
                with self.assertRaises(AssertionError): self.shell_contract_trace(mutated)

    def output_contract(self, library, repo, output):
        with tempfile.TemporaryDirectory() as tmp:
            lib=pathlib.Path(tmp)/"lib.sh"; lib.write_text(library)
            return subprocess.run(["bash","-c",'source "$1"; validate_output_dir "$2" "$3"',"bash",str(lib),str(repo),str(output)],text=True,capture_output=True)

    def test_in_repo_output_guard_is_causal_and_mutation_sensitive(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo=pathlib.Path(tmp)/"repo"; repo.mkdir(); subprocess.run(["git","init","-q",str(repo)],check=True)
            (repo/".gitignore").write_text("ignored/\n")
            library=SHELL_LIB.read_text()
            rejected=self.output_contract(library,repo,repo/"unignored"/"run")
            self.assertEqual(2,rejected.returncode); self.assertIn("must be git-ignored",rejected.stderr)
            accepted=self.output_contract(library,repo,repo/"ignored"/"run")
            self.assertEqual(0,accepted.returncode,accepted.stderr)
            mutated=library.replace('if [[ -z "$relative" ]] || ! git -C "$repository" check-ignore -q -- "$relative"; then','if false; then',1)
            self.assertNotEqual(library,mutated)
            incorrectly_accepted=self.output_contract(mutated,repo,repo/"unignored"/"run")
            self.assertEqual(0,incorrectly_accepted.returncode)

    def profile_guard_result(self, profile, library, ignored):
        with tempfile.TemporaryDirectory() as tmp:
            repo=pathlib.Path(tmp)/"repo"; scripts=repo/"scripts"; scripts.mkdir(parents=True)
            subprocess.run(["git","init","-q",str(repo)],check=True)
            (repo/".gitignore").write_text("ignored/\n")
            runner=scripts/"profile-durable-cross-binding-recovery.sh"; runner.write_text(profile)
            (scripts/"durable-cross-binding-recovery-lib.sh").write_text(library)
            prepare=scripts/"prepare-durable-cross-binding-run.py"
            prepare.write_text('#!/usr/bin/env python3\nimport sys\nprint("PREPARE_REACHED", file=sys.stderr)\nraise SystemExit(77)\n')
            jolt=repo/"fake-jolt"; jolt.write_text("#!/usr/bin/env bash\nexit 0\n"); jolt.chmod(0o755)
            native=repo/"libchdb.so"; native.write_bytes(b"stub")
            output=repo/("ignored" if ignored else "unignored")/"run"
            result=subprocess.run(["bash",str(runner),str(output),str(jolt),"e"*40,str(native),"5","16","1","1"],text=True,capture_output=True)
            return result,output.exists(),output.parent.exists()

    def assert_profile_rejects_unignored_output(self, profile, library):
        result,output_exists,parent_exists=self.profile_guard_result(profile,library,False)
        self.assertEqual(2,result.returncode,result.stderr)
        self.assertIn("must be git-ignored",result.stderr)
        self.assertNotIn("PREPARE_REACHED",result.stderr)
        self.assertFalse(output_exists,"rejected output directory was created")
        self.assertFalse(parent_exists,"rejected output parent was created")

    def test_profile_sources_and_invokes_output_guard_causally(self):
        profile=PROFILE.read_text(); library=SHELL_LIB.read_text()
        self.assert_profile_rejects_unignored_output(profile,library)
        accepted,output_exists,parent_exists=self.profile_guard_result(profile,library,True)
        self.assertEqual(77,accepted.returncode,accepted.stderr)
        self.assertIn("PREPARE_REACHED",accepted.stderr)
        self.assertTrue(output_exists); self.assertTrue(parent_exists)
        mutations=[
            ('source "$repo_root/scripts/durable-cross-binding-recovery-lib.sh"','source /dev/null'),
            ('validate_output_dir "$repo_root" "$output_dir"',':'),
            ('validate_output_dir "$repo_root" "$output_dir"\nmkdir -p "$output_dir" "$report_dir"',
             'mkdir -p "$output_dir" "$report_dir"\nvalidate_output_dir "$repo_root" "$output_dir"')]
        for old,new in mutations:
            with self.subTest(mutation=old):
                mutated=profile.replace(old,new,1)
                self.assertNotEqual(profile,mutated)
                with self.assertRaises(AssertionError):
                    self.assert_profile_rejects_unignored_output(mutated,library)

    def test_preparer_content_addresses_dirty_tracked_and_untracked_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp); repo=root/"repo"; reports=root/"reports"; repo.mkdir()
            subprocess.run(["git","init","-q",str(repo)],check=True)
            subprocess.run(["git","-C",str(repo),"config","user.email","oracle@example.invalid"],check=True)
            subprocess.run(["git","-C",str(repo),"config","user.name","Oracle Test"],check=True)
            (repo/"tracked.txt").write_text("initial\n"); subprocess.run(["git","-C",str(repo),"add","tracked.txt"],check=True); subprocess.run(["git","-C",str(repo),"commit","-qm","initial"],check=True)
            (repo/"tracked.txt").write_text("before\n"); subprocess.run(["git","-C",str(repo),"commit","-qam","base"],check=True)
            (repo/"tracked.txt").write_text("after\n"); (repo/"new.txt").write_text("new\n")
            result=subprocess.run(["python3",str(PREPARE),str(repo),str(reports),"object", "5","512","2","8"],text=True,capture_output=True)
            self.assertEqual(0,result.returncode,result.stderr)
            state=json.loads((reports/"harness-state.json").read_text()); self.assertEqual("dirty",state["status"]); self.assertGreater(state["tracked_patch"]["bytes"],0); self.assertEqual(["new.txt"],[x["path"] for x in state["untracked_files"]])
            unsigned=dict(state); claimed=unsigned.pop("state_sha256"); self.assertEqual(claimed,sha(canonical(unsigned)))

    def test_preparer_rejects_a_between_trial_source_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp); repo=root/"repo"; reports=root/"reports"; repo.mkdir()
            subprocess.run(["git","init","-q",str(repo)],check=True)
            subprocess.run(["git","-C",str(repo),"config","user.email","oracle@example.invalid"],check=True)
            subprocess.run(["git","-C",str(repo),"config","user.name","Oracle Test"],check=True)
            (repo/"tracked.txt").write_text("initial\n")
            subprocess.run(["git","-C",str(repo),"add","tracked.txt"],check=True)
            subprocess.run(["git","-C",str(repo),"commit","-qm","initial"],check=True)
            (repo/"tracked.txt").write_text("prepared\n")
            subprocess.run(["git","-C",str(repo),"commit","-qam","prepared"],check=True)
            prepared=subprocess.run(["python3",str(PREPARE),str(repo),str(reports),"object","5","512","2","8"],text=True,capture_output=True)
            self.assertEqual(0,prepared.returncode,prepared.stderr)
            verified=subprocess.run(["python3",str(PREPARE),"--verify-state",str(repo),str(reports)],text=True,capture_output=True)
            self.assertEqual(0,verified.returncode,verified.stderr)
            (repo/"tracked.txt").write_text("changed between trials\n")
            rejected=subprocess.run(["python3",str(PREPARE),"--verify-state",str(repo),str(reports)],text=True,capture_output=True)
            self.assertNotEqual(0,rejected.returncode)
            self.assertIn("current harness state differs",rejected.stderr)

if __name__ == "__main__": unittest.main()
