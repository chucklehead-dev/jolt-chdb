#!/usr/bin/env python3
"""Focused non-native contract tests for the data.json recovery A/B/B/A harness."""
import hashlib, json, pathlib, subprocess, tempfile, unittest

ROOT=pathlib.Path(__file__).resolve().parents[1]
SUMMARY=ROOT/"scripts/summarize-durable-data-json-recovery-abba.py"
PROFILE=ROOT/"scripts/profile-durable-data-json-recovery-abba.sh"
CAPTURE=ROOT/"scripts/capture-durable-data-json-provider.py"
NORMALIZE=ROOT/"scripts/normalize-durable-data-json-recovery-abba.py"
def canonical(x): return json.dumps(x,sort_keys=True,separators=(",", ":")).encode()
def sha(x): return hashlib.sha256(x).hexdigest()
def identity(name, n, char): return {"file_name":name,"bytes":n,"sha256":char*64}
def schedule():
    out=[]
    for c,o,p in (("A",None,"prime"),("B",None,"prime"),("A",1,"measured"),("B",1,"measured"),("B",2,"measured"),("A",2,"measured")):
        n=len(out); label="prime" if p=="prime" else str(o)
        out.append({"ordinal":n,"phase":p,"condition":c,"observation":o,"report_file":f"{c}-{label}.json","time_file":f"{c}-{label}.time"})
    return out

class AbbaSummaryTest(unittest.TestCase):
 def write(self,p,x): p.write_text(json.dumps(x))
 def corpus(self,root):
    r=root/"reports"; r.mkdir()
    state={"head":"a"*40,"tree":"b"*40,"state_sha256":"c"*64}
    common={"jolt":{"version":"jolt v0.8.6-1-g12345678","source_sha":"12345678"+"1"*32,"executable":identity("jolt",1,"2"),"executable_revision":"12345678","describe":identity("describe",1,"d")},"native":{"library":identity("libchdb.so",1,"3"),"header":identity("chdb.h",1,"4"),"version":"26.7.3"},"cache_scope":"condition-scoped-isolated-after-prime","harness":state}
    conditions={}
    for c,s in (("A","5"),("B","6")):
      conditions[c]={"condition":c,"checkout":{"root":"/tmp/"+c,"head":"7"*40,"tree":"8"*40},"data_json":{"sha":s*40,"root":"/tmp/data-"+c,"namespace":identity("json.cljc",1,"9")},**common}
    aggregate={"n":"52224","flags":"1","severity_sum":"2","body_bytes":"3","question_bodies":"4","min_trace":"a","max_trace":"z","min_span":"b","max_span":"y"}
    manifest={"schema_version":1,"fixture":{"rows":52224,"segments":3,"description":"current 52,224-row/3-segment fixture","inventory_sha256":"a"*64,"expected":aggregate,"expected_sha256":sha(canonical(aggregate))},"conditions":conditions,"schedule":schedule()}
    manifest["run_id"]=sha(canonical(manifest)); self.write(r/"run-manifest.json",manifest)
    for e in schedule():
      ns=1_000_000_000 if e["condition"]=="A" else 2_000_000_000
      report={"schema_version":1,"run_id":manifest["run_id"],"schedule_ordinal":e["ordinal"],"phase":e["phase"],"condition":e["condition"],"observation":e["observation"],"process_id":100+e["ordinal"],"process_started_epoch_ms":200+e["ordinal"],"process_finished_epoch_ms":201+e["ordinal"],"cache_condition":"warm-provider-cache-fresh-process-engine-and-scratch","fixture":{"inventory_sha256":"a"*64,"rows":52224,"segments":3,"inventory_unchanged":True},"provenance":conditions[e["condition"]],"recovery":{"elapsed_ns":ns,"rows_per_second":52224e9/ns,"expected":aggregate,"actual":aggregate}}
      self.write(r/e["report_file"],report); (r/e["time_file"]).write_text("Exit status: 0\n")
    return r
 def run_summary(self,r):
    out=r/"summary.json"; return subprocess.run(["python3",str(SUMMARY),str(r),str(out)],text=True,capture_output=True),out
 def test_directional_two_observation_summary(self):
    with tempfile.TemporaryDirectory() as d:
      result,out=self.run_summary(self.corpus(pathlib.Path(d))); self.assertEqual(0,result.returncode,result.stderr)
      summary=json.loads(out.read_text()); self.assertEqual(2,summary["semantics"]["observations_per_condition"]); self.assertAlmostEqual(.5,summary["directional"]["A_elapsed_over_B"]); self.assertNotIn("p99",summary["directional"])
 def test_rejects_schedule_pid_inventory_and_aggregate_mutants(self):
    cases=[("A-2.json",lambda x:x.update(process_id=102),"not fresh"),("B-2.json",lambda x:x["fixture"].update(inventory_unchanged=False),"fixture inventory/shape changed"),("A-1.json",lambda x:x["recovery"].update(actual={}),"missing or unknown fields"),("B-1.time",lambda x:None,"incomplete/failed")]
    for name,mutate,message in cases:
      with self.subTest(name=name),tempfile.TemporaryDirectory() as d:
       r=self.corpus(pathlib.Path(d))
       if name.endswith(".json"):
        p=r/name; v=json.loads(p.read_text()); mutate(v); self.write(p,v)
       else: (r/name).write_text("Exit status: 1\n")
       result,_=self.run_summary(r); self.assertNotEqual(0,result.returncode); self.assertIn(message,result.stderr)
 def test_rejects_cross_condition_compiler_or_native_drift(self):
    with tempfile.TemporaryDirectory() as d:
      r=self.corpus(pathlib.Path(d)); p=r/"run-manifest.json"; m=json.loads(p.read_text()); m["conditions"]["B"]["native"]["version"]="other"; m["run_id"]=sha(canonical({k:v for k,v in m.items() if k!="run_id"})); self.write(p,m)
      # Rebind reports to the tampered manifest so this reaches identity rather
      # than merely proving the run-id checksum control.
      for report in r.glob("[AB]-*.json"):
       x=json.loads(report.read_text()); x["run_id"]=m["run_id"]; self.write(report,x)
      result,_=self.run_summary(r); self.assertNotEqual(0,result.returncode); self.assertIn("compiler/native identity differs",result.stderr)
 def test_rejects_fixture_expected_aggregate_mutation(self):
    with tempfile.TemporaryDirectory() as d:
      r=self.corpus(pathlib.Path(d)); p=r/"A-1.json"; v=json.loads(p.read_text()); v["recovery"]["expected"]["flags"]="different"; v["recovery"]["actual"]["flags"]="different"; self.write(p,v)
      result,_=self.run_summary(r); self.assertNotEqual(0,result.returncode); self.assertIn("fixture expected aggregate differs",result.stderr)
 def test_profile_declares_fixed_shape_isolation_and_no_rust_comparison_claim(self):
    text=PROFILE.read_text()
    for token in ("current 52,224-row fixture", "condition-scoped", "capture-durable-data-json-provider.py", "A B A B B A", "none of its Rust recovery measurements participate"):
      self.assertIn(token,text)
    self.assertNotIn("p99",text)
 def test_profile_selects_actual_jolt_slots_and_rechecks_controls(self):
    text=PROFILE.read_text()
    self.assertIn("legacy_ordinals=(1 3 4 7 8 11)",text)
    self.assertIn("legacy_phases=(prime measured measured measured measured measured)",text)
    self.assertIn("legacy_trials=(none 1 2 3 4 5)",text)
    self.assertIn("verify_condition",text)
    self.assertIn("capture-durable-data-json-provider.py\" --verify",text)
    self.assertIn("-Srepro -Sdescribe > \"$output_dir/provider/$condition-sdescribe.edn\" || fail",text)
    self.assertIn("must differ only in deps.edn",text)
    self.assertIn('legacy_name=jolt-prime',text)
    self.assertIn('legacy_name="jolt-trial-${legacy_trials[$index]}"',text)
    self.assertIn('raw_receipt="$legacy_dir/$legacy_name.json"',text)
    self.assertIn('cmp -s "$output_dir/provider/$condition.json" "$resolved_provider"',text)
 def test_summary_rejects_banner_and_fixture_identity_mutants(self):
    with tempfile.TemporaryDirectory() as d:
      r=self.corpus(pathlib.Path(d)); p=r/"run-manifest.json"; m=json.loads(p.read_text()); m["conditions"]["A"]["jolt"]["executable_revision"]="deadbeef"; m["run_id"]=sha(canonical({k:v for k,v in m.items() if k!="run_id"})); self.write(p,m)
      result,_=self.run_summary(r); self.assertNotEqual(0,result.returncode); self.assertIn("banner/source binding differs",result.stderr)
    with tempfile.TemporaryDirectory() as d:
      r=self.corpus(pathlib.Path(d)); p=r/"run-manifest.json"; m=json.loads(p.read_text()); m["fixture"]["expected_sha256"]="0"*64; m["run_id"]=sha(canonical({k:v for k,v in m.items() if k!="run_id"})); self.write(p,m)
      result,_=self.run_summary(r); self.assertNotEqual(0,result.returncode); self.assertIn("expected aggregate identity differs",result.stderr)
 def test_provider_capture_rejects_noncanonical_empty_classpath(self):
    with tempfile.TemporaryDirectory() as d:
      root=pathlib.Path(d); checkout=root/"checkout"; checkout.mkdir(); subprocess.run(["git","init","-q",str(checkout)],check=True)
      fake=root/"jolt"; fake.write_text("#!/usr/bin/env bash\nif [[ $* == *-Spath* ]]; then printf '%s\\n' /tmp/no-provider; else exit 1; fi\n"); fake.chmod(0o755)
      out=root/"out.json"; result=subprocess.run(["python3",str(CAPTURE),str(checkout),str(fake),"a"*40,str(out)],text=True,capture_output=True)
      self.assertNotEqual(0,result.returncode); self.assertIn("exactly one canonical",result.stderr)
 def test_reresolved_provider_root_swap_has_a_distinct_capture(self):
    with tempfile.TemporaryDirectory() as d:
      root=pathlib.Path(d); provider1=root/"provider1"; provider2=root/"provider2"
      namespace=provider1/"src/main/clojure/clojure/data"; namespace.mkdir(parents=True); (namespace/"json.cljc").write_text("(ns clojure.data.json)\n")
      subprocess.run(["git","init","-q",str(provider1)],check=True); subprocess.run(["git","-C",str(provider1),"add","."],check=True); subprocess.run(["git","-C",str(provider1),"-c","user.name=x","-c","user.email=x@y","commit","-qm","provider"],check=True)
      # Make the two canonical roots represent the same exact source commit
      # while preserving different resolved-root evidence.
      subprocess.run(["git","clone","-q",str(provider1),str(provider2)],check=True)
      checkout=root/"checkout"; checkout.mkdir(); subprocess.run(["git","init","-q",str(checkout)],check=True)
      fake=root/"jolt"; fake.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"${PROVIDER_PATH}\"\n"); fake.chmod(0o755)
      expected=subprocess.check_output(["git","-C",str(provider1),"rev-parse","HEAD"],text=True).strip(); first=root/"first.json"; second=root/"second.json"
      for provider,out in ((provider1,first),(provider2,second)):
       result=subprocess.run(["python3",str(CAPTURE),str(checkout),str(fake),expected,str(out)],text=True,capture_output=True,env={"PROVIDER_PATH":str(provider/"src/main/clojure")})
       self.assertEqual(0,result.returncode,result.stderr)
      self.assertNotEqual(first.read_bytes(),second.read_bytes(),"root-aware capture must detect a swapped resolved provider")
 def test_normalizer_binds_raw_native_and_compiler_identity(self):
    # A synthetic old-format receipt cannot be relabeled with an outer
    # condition after its native/compiler identities have drifted.
    with tempfile.TemporaryDirectory() as d:
      root=pathlib.Path(d); raw=root/"raw.json"; outer=root/"outer.json"; out=root/"out.json"
      runtime={"runtime":"jolt","native_library":identity("lib",1,"1"),"native_header":identity("hdr",1,"2"),"native_version":"n","jolt_version":"jolt v0.8.6-1-g12345678","jolt_source_sha_asserted":"12345678"+"3"*32,"jolt_executable_revision":"12345678","jolt_sdescribe":identity("describe",1,"5"),"executable":identity("jolt",1,"4")}
      fixture={"inventory_sha256":"5"*64,"recovered_rows":52224,"wal_segments":3}
      aggregate={k:("52224" if k=="n" else "x") for k in ("n","flags","severity_sum","body_bytes","question_bodies","min_trace","max_trace","min_span","max_span")}
      self.write(raw,{"schema_version":1,"run_id":"x","schedule_ordinal":1,"phase":"prime","runtime":runtime,"trial":None,"process_id":1,"process_started_epoch_ms":1,"process_finished_epoch_ms":1,"cache_condition":"warm-provider-cache-fresh-process-engine-and-scratch","fixture":fixture,"recovery":{"elapsed_ns":1,"rows_per_second":1,"expected":aggregate,"actual":aggregate,"inventory_unchanged":True}})
      provenance={"conditions":{"A":{"native":{"library":identity("lib",1,"1"),"header":identity("hdr",1,"2"),"version":"different"},"jolt":{"version":runtime["jolt_version"],"source_sha":runtime["jolt_source_sha_asserted"],"executable_revision":runtime["jolt_executable_revision"],"describe":runtime["jolt_sdescribe"],"executable":runtime["executable"]}}},"run_id":"r"}; self.write(outer,provenance)
      result=subprocess.run(["python3",str(NORMALIZE),str(raw),str(outer),"A","none","prime","0","1","prime","none",str(outer),str(out)],text=True,capture_output=True)
      self.assertNotEqual(0,result.returncode); self.assertIn("compiler/native identity differs",result.stderr)
 def test_normalizer_rejects_selected_legacy_slot_mutation(self):
    with tempfile.TemporaryDirectory() as d:
      root=pathlib.Path(d); raw=root/"raw.json"; outer=root/"outer.json"; out=root/"out.json"
      executable=identity("jolt",1,"4"); describe=identity("describe",1,"5")
      runtime={"runtime":"jolt","native_library":identity("lib",1,"1"),"native_header":identity("hdr",1,"2"),"native_version":"n","jolt_version":"jolt v0.8.6-1-g12345678","jolt_source_sha_asserted":"12345678"+"3"*32,"jolt_executable_revision":"12345678","jolt_sdescribe":describe,"executable":executable}
      aggregate={k:("52224" if k=="n" else "x") for k in ("n","flags","severity_sum","body_bytes","question_bodies","min_trace","max_trace","min_span","max_span")}
      self.write(raw,{"schema_version":1,"run_id":"x","schedule_ordinal":4,"phase":"measured","runtime":runtime,"trial":2,"process_id":1,"process_started_epoch_ms":1,"process_finished_epoch_ms":1,"cache_condition":"warm-provider-cache-fresh-process-engine-and-scratch","fixture":{"inventory_sha256":"5"*64,"recovered_rows":52224,"wal_segments":3},"recovery":{"elapsed_ns":1,"rows_per_second":1,"expected":aggregate,"actual":aggregate,"inventory_unchanged":True}})
      self.write(outer,{"conditions":{"A":{"native":{"library":runtime["native_library"],"header":runtime["native_header"],"version":"n"},"jolt":{"version":runtime["jolt_version"],"source_sha":runtime["jolt_source_sha_asserted"],"executable_revision":"12345678","describe":describe,"executable":executable}}},"run_id":"r"})
      result=subprocess.run(["python3",str(NORMALIZE),str(raw),str(outer),"A","1","measured","2","3","measured","1",str(outer),str(out)],text=True,capture_output=True)
      self.assertNotEqual(0,result.returncode); self.assertIn("ordinal/phase/trial differs",result.stderr)

if __name__=="__main__": unittest.main()
