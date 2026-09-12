#!/usr/bin/env python3
import copy, hashlib, json, pathlib, subprocess, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SUMMARY = ROOT / "scripts/summarize-durable-cross-binding-recovery.py"
PREPARE = ROOT / "scripts/prepare-durable-cross-binding-run.py"
PROFILE = ROOT / "scripts/profile-durable-cross-binding-recovery.sh"
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
        jolt={"runtime":"jolt","jolt_version":"0.8.6","jolt_source_sha":"e"*64,"scheme_version":"10.4.1","machine_type":"ta6le","native_version":"26.7.2","harness_state":state,"native_library":native,"native_header":header,"executable":{"file_name":"joltc","bytes":20,"sha256":"f"*64}}
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
          ("jolt-trial-1.json",lambda x:x["runtime"].update(jolt_source_sha="short"),"Jolt SHA"),
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

if __name__ == "__main__": unittest.main()
