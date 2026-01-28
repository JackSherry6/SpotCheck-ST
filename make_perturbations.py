import pandas as pd
from itertools import product
import json, hashlib
from pathlib import Path
from config import CONFIG

def make_run_id(params):
    s = json.dumps(params, sort_keys=True)
    return hashlib.md5(s.encode()).hexdigest()[:8]

def estimate_memory_requirement(row):
    base_mem = 4  # GB baseline
    
    # Factor 1: smoothing (biggest memory consumer due to dense matrix ops)
    smooth_mem = row['smooth'] * 2.0
    
    # Factor 2: neighborhood size (affects adjacency matrix size)
    if row['n_neighbors'] >= 20:
        neighbor_mem = 2.0
    elif row['n_neighbors'] >= 15:
        neighbor_mem = 1.0
    else:
        neighbor_mem = 0.5
    
    # Factor 3: QC stringency (Lower quantiles = more spots retained = more memory)
    avg_qc = (row['gene_q'] + row['umi_q']) / 2
    if avg_qc <= 0.05:
        qc_mem = 1.5 
    elif avg_qc <= 0.075:
        qc_mem = 1.0
    else:
        qc_mem = 0.5 
    
    total_mem = base_mem + smooth_mem + neighbor_mem + qc_mem
    
    # round up to nearest standard allocation: 8, 16, 32 GB (gave cusion of -1 for each just to be safe)
    if total_mem <= 7:
        return 8
    elif total_mem <= 15:
        return 16
    else:
        return 32

# Generate all perturbation combinations
rows = []
for gene_q, umi_q, norm, smooth, k, res in product(
    CONFIG["qc"]["gene_quantiles"],
    CONFIG["qc"]["umi_quantiles"],
    CONFIG["normalization"]["methods"],
    CONFIG["spatial"]["smoothing_steps"],
    CONFIG["graph"]["n_neighbors"],
    CONFIG["graph"]["leiden_res"],
):
    params = dict(
        gene_q=gene_q,
        umi_q=umi_q,
        norm=norm,
        smooth=smooth,
        n_neighbors=k,
        leiden_res=res,
    )
    params["run_id"] = make_run_id(params)
    rows.append(params)

df = pd.DataFrame(rows)

df['estimated_mem_gb'] = df.apply(estimate_memory_requirement, axis=1)

df.to_parquet("perturbations.parquet", index=False)
print(f"Wrote {len(df)} perturbations")

print("\n" + "="*70)
print("MEMORY REQUIREMENT ANALYSIS")
print("="*70)

mem_counts = df['estimated_mem_gb'].value_counts().sort_index()
print(f"\nTotal perturbations: {len(df)}")
print("\nMemory tier breakdown:")
for mem_gb, count in mem_counts.items():
    pct = 100 * count / len(df)
    print(f"  {mem_gb:2d} GB: {count:4d} tasks ({pct:5.1f}%)")

print("\nCreating memory-stratified index files...")
for mem_gb in sorted(df['estimated_mem_gb'].unique()):
    tier_df = df[df['estimated_mem_gb'] == mem_gb].reset_index(drop=True)
    output_file = f"perturbations_{mem_gb}gb_indices.txt"
    tier_df[['run_id']].to_csv(output_file, index=False, header=False)
    print(f"  Created {output_file}: {len(tier_df)} tasks")

# 32 GB tasks should be avoided but this is here for security
if 32 in mem_counts.index:
    print("\n" + "-"*70)
    print("HIGH MEMORY (32 GB) TASKS - Parameter patterns:")
    print("-"*70)
    high_mem = df[df['estimated_mem_gb'] == 32]
    
    print("\nBy smoothing steps:")
    for smooth, count in high_mem['smooth'].value_counts().sort_index().items():
        print(f"  smooth={smooth}: {count} tasks")
    
    print("\nBy n_neighbors:")
    for k, count in high_mem['n_neighbors'].value_counts().sort_index().items():
        print(f"  n_neighbors={k}: {count} tasks")

print("\n" + "="*70)
print("✓ Perturbations and memory allocation complete!")
print("="*70)
