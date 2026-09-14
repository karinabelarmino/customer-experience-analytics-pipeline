"""Exact result grid styled after the supplied SIGN-RESTRICTED BVAR figure.

Regenerates vector shapes and CSV-backed text; does not modify a raster image.
Matplotlib is optional and needed only to regenerate the delivered PNG/SVG files.
"""
from pathlib import Path
import csv
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["svg.fonttype"] = "none"
matplotlib.rcParams["svg.hashsalt"] = "cx-exercise-02-bvar-reference"
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle, FancyArrowPatch
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parent
BG, WHITE, INK = "#EFF0F2", "#FDFDFD", "#202323"
BODY, MUTED, BORDER = "#414546", "#666A6D", "#85888A"
GRID, HEADER = "#D7DADD", "#F4F5F5"
RED, PALE_RED, RED_SHADOW = "#C53747", "#FFF5F5", "#EFA8B0"
GRAY_SHADOW = "#D3D6D8"

# Nimbus Sans approximates the supplied reference; portable fallback is bundled
# with Matplotlib. Font selection only affects rendering, never data semantics.
FAMILIES = {f.name for f in font_manager.fontManager.ttflist}
BODY_FONT = "Nimbus Sans" if "Nimbus Sans" in FAMILIES else "DejaVu Sans"
TITLE_FONT = "Nimbus Sans Narrow" if "Nimbus Sans Narrow" in FAMILIES else BODY_FONT


def draw(language):
    pt = language == "pt"
    with (ROOT / "data/diagnostics/micro_before.csv").open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 8
    assert sum(r["response_id"] == "R001" for r in rows) == 6
    assert {r["score"] for r in rows if r["response_id"] == "R001"} == {"3"}
    fig = plt.figure(figsize=(8, 8), dpi=200, facecolor=BG)
    ax = fig.add_axes([0, 0, 1, 1]); ax.set_xlim(0, 800); ax.set_ylim(800, 0); ax.axis("off")

    def text(x, y, value, size=11, color=BODY, weight="normal", font=BODY_FONT, **kwargs):
        return ax.text(x, y, value, fontsize=size, color=color, weight=weight,
                       va="top", fontfamily=font, **kwargs)

    def box(x, y, w, h, fill=WHITE, edge=BORDER, shadow=GRAY_SHADOW, radius=15):
        if shadow:
            ax.add_patch(FancyBboxPatch((x, y + 5), w, h,
                boxstyle=f"round,pad=0,rounding_size={radius}", facecolor=shadow, linewidth=0))
        ax.add_patch(FancyBboxPatch((x, y), w, h,
            boxstyle=f"round,pad=0,rounding_size={radius}", facecolor=fill,
            edgecolor=edge if edge else fill, linewidth=.8 if edge else 0))

    def arrow(y0, y1):
        ax.add_patch(FancyArrowPatch((400, y0), (400, y1), arrowstyle="->",
                                    mutation_scale=10, linewidth=1, color="#5F6264"))

    # The same white sheet / soft gray surround as the supplied reference.
    for offset, alpha in [(7, .06), (4, .09), (2, .12)]:
        ax.add_patch(FancyBboxPatch((24-offset/2, 18+offset), 752+offset, 764,
            boxstyle="round,pad=0,rounding_size=8", facecolor="#B8BCC0", alpha=alpha, linewidth=0))
    box(24, 18, 752, 764, edge=None, shadow=None, radius=7)
    text(400, 30, "RESPONSE GRAIN", 28, INK, "bold", TITLE_FONT, ha="center")
    text(400, 80, "REPEATED EVENTS AND DEDUPLICATION", 8.7, MUTED, "bold", ha="center")

    box(126, 117, 548, 87)
    text(400, 135, "1. PIPELINE EXECUTADO COM SUCESSO" if pt else
         "1. PIPELINE COMPLETED SUCCESSFULLY", 13.6, INK, "bold", TITLE_FONT, ha="center")
    text(400, 170, "Abri a tabela e os dados vieram assim." if pt else
         "I opened the table. This is what I found.", 11, BODY, ha="center")
    arrow(210, 232)

    box(56, 242, 688, 407)
    text(400, 260, "2. RESULTADO NA TABELA" if pt else "2. TABLE RESULTS",
         14, INK, "bold", TITLE_FONT, ha="center")
    text(400, 290, "SELECT * FROM cx_responses;", 9.5, MUTED,
         font="DejaVu Sans Mono", ha="center")

    # Four actual source columns; the leftmost gutter is a display row number.
    x, y = 74, 320
    widths = [30, 133, 58, 216, 215]
    starts = [x]
    for width in widths:
        starts.append(starts[-1] + width)
    header_h, row_h = 33, 31
    headers = ["", "response_id", "score", "category", "question_id"]
    ax.add_patch(Rectangle((x, y), sum(widths), header_h, facecolor=HEADER,
                           edgecolor=GRID, linewidth=.5))
    for i, header in enumerate(headers):
        text(starts[i] + 9, y + 10, header, 9.5, BODY, "bold")
    translations = {"Atendimento": "Service", "Comunicação": "Communication", "Processo": "Process"}
    cell_artists = []
    for idx, row in enumerate(rows):
        yy = y + header_h + idx * row_h
        ax.add_patch(Rectangle((x, yy), sum(widths), row_h, facecolor=WHITE, linewidth=0))
        repeated = row["response_id"] == "R001"
        if repeated:
            ax.add_patch(Rectangle((starts[1], yy), widths[1]+widths[2], row_h,
                                   facecolor=PALE_RED, linewidth=0))
        values = [str(idx+1), row["response_id"], row["score"],
                  row["category"] if pt else translations.get(row["category"], row["category"]),
                  row["question_id"]]
        for col, value in enumerate(values):
            value = value or "null"
            color = RED if repeated and col in (1, 2) else BODY
            if col == 0 or value == "null":
                color = "#929799"
            artist = text(starts[col]+9, yy+9, value, 10.8 if col else 9,
                          color, "bold" if repeated and col in (1, 2) else "normal",
                          style="italic" if value == "null" else "normal")
            cell_artists.append((artist, starts[col+1]-5))
        ax.plot([x, starts[-1]], [yy+row_h, yy+row_h], color=GRID, linewidth=.4)
    for pos in starts:
        ax.plot([pos, pos], [y, y+header_h+row_h*8], color=GRID, linewidth=.4)
    ax.add_patch(Rectangle((starts[1], y+header_h), widths[1]+widths[2], row_h*6,
                          fill=False, edgecolor=RED, linewidth=.7))
    text(400, 620, "8 linhas exibidas · mesmo ID e mesma nota em 6 delas" if pt else
         "8 rows shown · the same ID and score appear in 6 of them", 9.5, MUTED, ha="center")

    arrow(655, 677)
    box(208, 684, 384, 65, fill=PALE_RED, edge="#D86775", shadow=RED_SHADOW, radius=14)
    text(400, 703, "O QUE VOCÊ FARIA?" if pt else "WHAT WOULD YOU DO?",
         18, RED, "bold", TITLE_FONT, ha="center")
    text(400, 766, "Dados sintéticos. Visualização ilustrativa de uma tabela." if pt else
         "Synthetic data. Illustrative table view.", 8.5, MUTED, ha="center")

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    for artist, boundary in cell_artists:
        right = ax.transData.inverted().transform((artist.get_window_extent(renderer).x1, 0))[0]
        assert right < boundary, artist.get_text()
    for artist in ax.texts:
        bounds = artist.get_window_extent(renderer)
        left = ax.transData.inverted().transform((bounds.x0, 0))[0]
        right = ax.transData.inverted().transform((bounds.x1, 0))[0]
        assert 40 < left and right < 760, artist.get_text()
    for ext in ("png", "svg"):
        fig.savefig(ROOT / f"assets/response-grain-{language}.{ext}", dpi=200, facecolor=BG,
                    metadata={"Date": None} if ext == "svg" else None)
    plt.close(fig)


if __name__ == "__main__":
    draw("pt"); draw("en")
    print("Saved Portuguese and English result grids: PNG + SVG, 1600 x 1600.")
