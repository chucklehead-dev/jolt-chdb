#!/usr/bin/env python3
"""Write the signed outer A/B/B/A plan after shell has captured both providers."""
import hashlib, json, pathlib, subprocess, sys
def fail(message): raise SystemExit("data.json recovery A/B/B/A preparation failed: " + message)
def canonical(x): return json.dumps(x, sort_keys=True, separators=(",", ":")).encode()
def sha(x): return hashlib.sha256(x).hexdigest()
def git(root, arg): return subprocess.check_output(["git","-C",str(root),"rev-parse",arg],text=True).strip()
def state(root):
    return {"head":git(root,"HEAD"),"tree":git(root,"HEAD^{tree}"),"state_sha256":sha((git(root,"HEAD")+git(root,"HEAD^{tree}")).encode())}
def schedule():
    out=[]
    for c,o,p in (("A",None,"prime"),("B",None,"prime"),("A",1,"measured"),("B",1,"measured"),("B",2,"measured"),("A",2,"measured")):
        n=len(out); label="prime" if p=="prime" else str(o)
        out.append({"ordinal":n,"phase":p,"condition":c,"observation":o,"report_file":f"{c}-{label}.json","time_file":f"{c}-{label}.time"})
    return out
def identity(path):
    data=path.read_bytes(); return {"file_name":path.name,"bytes":len(data),"sha256":sha(data)}
def validate_describe(path, version, source_sha):
    text=path.read_text()
    revision=version.rsplit("-g",1)[-1] if "-g" in version else ""
    if not version.startswith("jolt v") or len(revision)<8 or not source_sha.startswith(revision):
        fail("Jolt banner revision does not agree with asserted source SHA")
    for token in (':project-dir "."', ':config-user nil', ':config-project "./deps.edn"', ':repro true', ':aliases []'):
        if token not in text: fail("Jolt Sdescribe is not exact project-only repro provenance")
    return {"executable_revision":revision,"describe":identity(path)}
def main():
    if len(sys.argv)!=14: fail("usage: prepare... REPORT_DIR FIXTURE A_CHECKOUT B_CHECKOUT A_PROVIDER B_PROVIDER JOLT_BIN JOLT_SHA LIBCHDB HEADER JOLT_VERSION A_DESCRIBE B_DESCRIBE")
    reports,fixture,a,b,ap,bp,jolt,_,lib,header,_,ad,bd=map(pathlib.Path,sys.argv[1:])
    jolt_sha=sys.argv[8]; version=sys.argv[11]
    if not jolt_sha or len(jolt_sha)!=40 or any(c not in "0123456789abcdef" for c in jolt_sha): fail("Jolt source SHA is invalid")
    if not version.startswith("jolt v"): fail("Jolt banner is invalid")
    if not jolt.is_file() or not lib.is_file() or not header.is_file() or not ad.is_file() or not bd.is_file(): fail("Jolt/native/describe identity file is missing")
    reports.mkdir(parents=True,exist_ok=False)
    try:
        providers={"A":json.loads(ap.read_text()),"B":json.loads(bp.read_text())}
        fixture_value=json.loads(fixture.read_text())
    except (OSError,json.JSONDecodeError) as error: fail("cannot read provider capture: "+str(error))
    if providers["A"].get("sha")==providers["B"].get("sha"): fail("A and B data.json SHAs must differ")
    conditions={}
    for name,root in (("A",a),("B",b)):
        if subprocess.check_output(["git","-C",str(root),"status","--porcelain"],text=True): fail(name+" checkout is not clean")
        conditions[name]={"condition":name,"checkout":{"root":str(root.resolve()),"head":git(root,"HEAD"),"tree":git(root,"HEAD^{tree}")},"data_json":providers[name],"jolt":None,"native":None,"cache_scope":"condition-scoped-isolated-after-prime","harness":state(root)}
    try: inventory=fixture_value["inventory_sha256"]
    except KeyError: fail("fixture descriptor lacks inventory identity")
    executable=identity(jolt); native={"library":identity(lib),"header":identity(header),"version":fixture_value.get("producer",{}).get("native_version")}
    if not native["version"]: fail("fixture descriptor lacks native version")
    for condition, describe in (("A", ad), ("B", bd)):
        describe_metadata=validate_describe(describe, version, jolt_sha)
        conditions[condition]["jolt"]={"version":version,"source_sha":jolt_sha,"executable":executable, **describe_metadata}
        conditions[condition]["native"]=native
    expected=fixture_value.get("expected")
    if not isinstance(expected,dict) or set(expected)!={"n","flags","severity_sum","body_bytes","question_bodies","min_trace","max_trace","min_span","max_span"}: fail("fixture descriptor lacks exact expected aggregate")
    manifest={"schema_version":1,"fixture":{"rows":52224,"segments":3,"description":"current 52,224-row/3-segment fixture","inventory_sha256":inventory,"expected":expected,"expected_sha256":sha(canonical(expected))},"conditions":conditions,"schedule":schedule()}
    manifest["run_id"]=sha(canonical(manifest))
    (reports/"run-manifest.json").write_text(json.dumps(manifest,sort_keys=True)+"\n")
if __name__=="__main__": main()
