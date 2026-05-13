import json
import os
from collections import Counter
from itertools import combinations

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.gridspec import GridSpec

mpl.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10,
    "axes.spines.top": False, "axes.spines.right": False,
    "legend.frameon": False, "savefig.dpi": 300, "savefig.bbox": "tight",
})

COL = {"d": "#1f4e79", "p": "#c0504d", "s": "#7e6b8f", "c": "#e8a33d", "m": "#4f7a4d"}
LABELS = {"d": "Delegitimization", "p": "Polarization", "s": "Scapegoating",
          "c": "Conspiracy", "m": "Anti-media"}
CATS = ["d", "p", "s", "c", "m"]
OUT = "figures"
os.makedirs(OUT, exist_ok=True)


def save(fig, name):
    for ext in ("png", "pdf", "svg"):
        fig.savefig(f"{OUT}/{name}.{ext}")
    plt.close(fig)


with open("final_dedup_coded.json") as f:
    data = json.load(f)
data = [r for r in data if 2000 <= r["year"] <= 2024]
N = len(data)

df = pd.DataFrame(data)
df["ws"] = df[CATS].sum(axis=1)
years = sorted(df["year"].unique())

# combined panel: yearly category rates + mean weaponization
fig = plt.figure(figsize=(9, 6.5))
gs = GridSpec(2, 1, height_ratios=[2.2, 1], hspace=0.18)

ax1 = fig.add_subplot(gs[0])
yearly = df.groupby("year")[CATS].mean() * 100
for c in CATS:
    ax1.plot(yearly.index, yearly[c], marker="o", markersize=3.5, linewidth=1.6,
             label=LABELS[c], color=COL[c])
ax1.set_ylabel("Share of snippets (%)")
ax1.legend(loc="upper right", ncol=2, fontsize=9)
ax1.grid(axis="y", linestyle=":", alpha=0.4)
ax1.set_ylim(0, 100)
ax1.set_xticks([y for y in years if y % 2 == 0])
ax1.set_xticklabels([])
ax1.set_xlim(min(years) - 0.5, max(years) + 0.5)

ax2 = fig.add_subplot(gs[1], sharex=ax1)
ws_y = df.groupby("year")["ws"].mean()
ax2.plot(ws_y.index, ws_y.values, marker="o", markersize=4, linewidth=1.8, color="#222")
ax2.fill_between(ws_y.index, ws_y.values, alpha=0.12, color="#222")
m = df["ws"].mean()
ax2.axhline(m, linestyle="--", color="#c0504d", linewidth=1.1, label=f"Mean = {m:.2f}")
ax2.set_xlabel("Year")
ax2.set_ylabel("Mean weaponization\nscore (0–5)")
ax2.set_xticks([y for y in years if y % 2 == 0])
ax2.tick_params(axis="x", rotation=45)
ax2.grid(axis="y", linestyle=":", alpha=0.4)
ax2.legend(loc="upper right", fontsize=9)
save(fig, "fig1_categories_and_ws")

# period bars
fig, ax = plt.subplots(figsize=(8, 5))
x = np.arange(len(CATS))
w = 0.36
era1 = [r for r in data if r["year"] <= 2014]
era2 = [r for r in data if r["year"] >= 2015]
r1 = [sum(r[c] for r in era1)/len(era1)*100 for c in CATS]
r2 = [sum(r[c] for r in era2)/len(era2)*100 for c in CATS]
ax.bar(x - w/2, r1, w, label=f"2000-2014 (n={len(era1)})", color="#7a96b8")
ax.bar(x + w/2, r2, w, label=f"2015-2024 (n={len(era2)})", color="#c98b88")
ax.set_xticks(x)
ax.set_xticklabels([LABELS[c] for c in CATS], rotation=15, ha="right")
ax.set_ylabel("Share of snippets (%)")
ax.legend()
for i, (a, b) in enumerate(zip(r1, r2)):
    ax.text(i - w/2, a + 1, f"{a:.1f}%", ha="center", fontsize=8.5)
    ax.text(i + w/2, b + 1, f"{b:.1f}%", ha="center", fontsize=8.5)
ax.grid(axis="y", linestyle=":", alpha=0.4)
save(fig, "fig2_period_comparison")

# co-occurrence heatmap
M = np.zeros((5, 5), dtype=int)
for i, v1 in enumerate(CATS):
    for j, v2 in enumerate(CATS):
        if v1 == v2:
            M[i, j] = sum(r[v1] for r in data)
        else:
            M[i, j] = sum(1 for r in data if r[v1] == 1 and r[v2] == 1)

fig, ax = plt.subplots(figsize=(7, 6))
im = ax.imshow(M, cmap="Blues")
ax.set_xticks(range(5)); ax.set_yticks(range(5))
ax.set_xticklabels([LABELS[c] for c in CATS], rotation=30, ha="right")
ax.set_yticklabels([LABELS[c] for c in CATS])
for i in range(5):
    for j in range(5):
        color = "white" if M[i, j] > M.max() * 0.5 else "#222"
        ax.text(j, i, str(M[i, j]), ha="center", va="center", color=color, fontsize=10)
plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="Snippets")
save(fig, "fig3_cooccurrence")

# top pairs
pairs = Counter()
for r in data:
    active = [v for v in CATS if r[v] == 1]
    for a, b in combinations(active, 2):
        pairs[(a, b)] += 1

top = pairs.most_common(10)
labs = [f"{LABELS[a]} + {LABELS[b]}" for (a, b), _ in top]
vals = [v for _, v in top]

fig, ax = plt.subplots(figsize=(9, 5))
yp = list(range(len(labs)))[::-1]
ax.barh(yp, vals, color="#1f4e79", height=0.65)
ax.set_yticks(yp); ax.set_yticklabels(labs)
ax.set_xlabel("Snippets")
for y, v in zip(yp, vals):
    ax.text(v + 4, y, f"{v} ({v/N*100:.1f}%)", va="center", fontsize=9)
ax.grid(axis="x", linestyle=":", alpha=0.4)
ax.set_xlim(0, max(vals) * 1.18)
save(fig, "fig4_top_pairs")

# categories per snippet
counts = Counter(sum(r[c] for c in CATS) for r in data)
ks = sorted(counts.keys())
vs = [counts[k] for k in ks]

fig, ax = plt.subplots(figsize=(7.5, 4.8))
cols = ["#bcbcbc", "#1f4e79", "#3a6ea5", "#5a8fc7", "#7eb1e0", "#a3d0f4"]
ax.bar([str(k) for k in ks], vs, color=cols[:len(ks)])
ax.set_xlabel("Number of categories per snippet")
ax.set_ylabel("Snippets")
for i, v in enumerate(vs):
    ax.text(i, v + N*0.005, f"{v}\n({v/N*100:.1f}%)", ha="center", fontsize=9)
ax.grid(axis="y", linestyle=":", alpha=0.4)
save(fig, "fig5_categories_per_snippet")
