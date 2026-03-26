(function () {
  "use strict";

  var data = null;
  var storageAnalysis = null;
  var duckdbContext = null;
  var currentQuery = null;
  var currentStorageTable = null;
  var currentIteration = 0;
  var currentStage = 0;
  var ENABLE_BRANCHING_PLAN_VIEW = false;
  var THEME_STORAGE_KEY = "bespoke-demo-theme";

  var $ = function (id) { return document.getElementById(id); };

  var STAGE_ICONS = {
    search: "\u{1F50D}",
    database: "\u{1F4BE}",
    code: "\u{1F4BB}",
    zap: "\u26A1",
    rocket: "\u{1F680}",
    cpu: "\u{1F9E0}"
  };

  function getStageIcon(stage) {
    var stageName = String((stage && stage.stage) || "").toLowerCase();
    if (stageName === "query analysis") return STAGE_ICONS.search;
    if (stageName === "storage design") return STAGE_ICONS.database;
    if (stageName === "base") return STAGE_ICONS.code;
    if (stageName === "base optimization") return STAGE_ICONS.zap;
    if (stageName === "human persona") return STAGE_ICONS.rocket;
    return STAGE_ICONS[(stage && stage.icon) || ""] || "\u2022";
  }

  // ── Init ──
  async function init() {
    initTheme();
    await loadData("data/tpch-sf20.json");
    initSettingsPopover();
    $("sel-benchmark").addEventListener("change", function () {
      loadData("data/" + $("sel-benchmark").value + ".json");
      closeSettingsPopover();
    });
    $("sel-theme").addEventListener("change", function () {
      applyTheme($("sel-theme").value);
      closeSettingsPopover();
    });
    $("sel-query").addEventListener("change", function () {
      loadQuery($("sel-query").value);
      window.location.hash = data.benchmark + "/" + $("sel-query").value.toLowerCase();
    });
    window.addEventListener("hashchange", applyHash);
    setupRevealObserver();
    applyHash();
  }

  function initSettingsPopover() {
    var button = $("settings-button");
    var popover = $("settings-popover");
    if (!button || !popover) return;

    button.addEventListener("click", function (event) {
      event.stopPropagation();
      var isOpen = !popover.hasAttribute("hidden");
      if (isOpen) {
        closeSettingsPopover();
      } else {
        openSettingsPopover();
      }
    });

    popover.addEventListener("click", function (event) {
      event.stopPropagation();
    });

    document.addEventListener("click", function () {
      closeSettingsPopover();
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape") {
        closeSettingsPopover();
      }
    });
  }

  function openSettingsPopover() {
    var button = $("settings-button");
    var popover = $("settings-popover");
    if (!button || !popover) return;
    popover.removeAttribute("hidden");
    button.setAttribute("aria-expanded", "true");
  }

  function closeSettingsPopover() {
    var button = $("settings-button");
    var popover = $("settings-popover");
    if (!button || !popover) return;
    popover.setAttribute("hidden", "");
    button.setAttribute("aria-expanded", "false");
  }

  async function loadData(url) {
    try {
      var responses = await Promise.all([
        fetch(url),
        fetch("data/storage_plan_analysis.json").catch(function () { return null; }),
        fetch("data/tpch-sf20-duckdb-context.json").catch(function () { return null; })
      ]);
      data = await responses[0].json();
      storageAnalysis = null;
      duckdbContext = null;
      if (responses[1] && responses[1].ok) {
        storageAnalysis = await responses[1].json();
      }
      if (responses[2] && responses[2].ok) {
        duckdbContext = await responses[2].json();
      }
      syncBenchmarkLabel();
      syncWandbRunId();
      populateQuerySelector();
    } catch (e) {
      console.error("Failed to load data:", e);
    }
  }

  function initTheme() {
    var savedTheme = null;
    try {
      savedTheme = window.localStorage.getItem(THEME_STORAGE_KEY);
    } catch (e) {}
    var theme = savedTheme || "light";
    if (theme === "colour") theme = "light";
    if ($("sel-theme")) {
      $("sel-theme").value = theme;
    }
    applyTheme(theme);
  }

  function applyTheme(theme) {
    var nextTheme = theme || "light";
    if (nextTheme === "colour") nextTheme = "light";
    document.body.setAttribute("data-theme", nextTheme);
    if ($("sel-theme") && $("sel-theme").value !== nextTheme) {
      $("sel-theme").value = nextTheme;
    }
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, nextTheme);
    } catch (e) {}
  }

  function syncBenchmarkLabel() {
    var sel = $("sel-benchmark");
    if (!sel || !data) return;
    var option = sel.options[sel.selectedIndex];
    if (!option) return;
    var benchmarkName = (data.benchmark || "tpch").toUpperCase();
    var scaleFactor = data.scaleFactor != null ? data.scaleFactor : "?";
    option.textContent = benchmarkName + " SF" + scaleFactor;
  }

  function syncWandbRunId() {
    var input = $("wandb-run-id");
    if (!input || !data) return;
    input.value = data.wandbRunId || data.runId || "";
  }

  function populateQuerySelector() {
    var sel = $("sel-query");
    var rail = $("query-rail");
    sel.innerHTML = "";
    if (rail) rail.innerHTML = "";
    var ids = Object.keys(data.queries).sort(function (a, b) {
      return parseInt(a.replace(/\D/g, ""), 10) - parseInt(b.replace(/\D/g, ""), 10);
    });
    ids.forEach(function (qid) {
      var query = data.queries[qid];
      var opt = document.createElement("option");
      opt.value = qid;
      opt.textContent = qid;
      sel.appendChild(opt);

      if (rail) {
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "query-rail-btn";
        btn.setAttribute("data-query-id", qid);
        btn.innerHTML =
          '<span class="query-rail-id">' + escHtml(qid) + '</span>' +
          '<span class="query-rail-name">' + escHtml(query.name) + '</span>' +
          '<span class="query-rail-meta">' +
            '<span class="query-rail-speedup">' + escHtml(query.finalSpeedup.toFixed(1) + "x") + '</span>' +
            '<span class="query-rail-meta-label">vs DuckDB</span>' +
          '</span>';
        btn.addEventListener("click", function () {
          $("sel-query").value = qid;
          loadQuery(qid);
          window.location.hash = data.benchmark + "/" + qid.toLowerCase();
          window.scrollTo({ top: 0, behavior: "smooth" });
        });
        rail.appendChild(btn);
      }
    });
  }

  function applyHash() {
    var hash = window.location.hash.replace("#", "");
    if (hash) {
      var parts = hash.split("/");
      if (parts.length >= 2) {
        var qid = parts[1].toUpperCase();
        if (data && data.queries[qid]) $("sel-query").value = qid;
      }
    }
    loadQuery($("sel-query").value);
  }

  // ── Load Query ──
  function loadQuery(qid) {
    if (!data || !data.queries[qid]) return;
    currentQuery = qid;
    currentStorageTable = null;
    var q = data.queries[qid];
    currentStage = Math.max(0, Math.min(currentStage, (q.pipeline || []).length - 1));
    currentIteration = deriveIterationForStage(q, currentStage);

    $("query-name").textContent = q.name;
    $("sf-label").textContent = "SF" + data.scaleFactor;
    $("iter-count").textContent = q.iterations.length + " iterations";

    renderPipelineNav(q);
    renderStageContent(q, currentStage);
    renderPerfChart(q);
    renderTimeline(q);
    renderHeatmap();
    renderQueryRailState();

    triggerReveals();
    requestAnimationFrame(function () {
      requestAnimationFrame(animatePerfBars);
    });
  }

  function deriveIterationForStage(q, stageIdx) {
    if (!q || !q.iterations || !q.iterations.length) return 0;
    if (stageIdx <= 1) return q.iterations.length - 1;
    return Math.max(0, Math.min(stageIdx - 2, q.iterations.length - 1));
  }

  function renderQueryRailState() {
    var rail = $("query-rail");
    if (!rail) return;
    rail.querySelectorAll(".query-rail-btn").forEach(function (btn) {
      var isActive = btn.getAttribute("data-query-id") === currentQuery;
      btn.classList.toggle("active", isActive);
      if (isActive) {
        btn.scrollIntoView({ inline: "center", block: "nearest", behavior: "smooth" });
      }
    });
  }

  // ── Pipeline Navigation ──
  function renderPipelineNav(q) {
    var nav = $("pipeline-nav");
    nav.innerHTML = "";
    if (!q.pipeline) return;

    q.pipeline.forEach(function (stage, idx) {
      var btn = document.createElement("button");
      btn.className = "pipeline-btn" + (idx === currentStage ? " active" : "");

      var icon = document.createElement("span");
      icon.className = "pipeline-btn-icon";
      icon.textContent = getStageIcon(stage);
      btn.appendChild(icon);

      var label = document.createTextNode(stage.stage);
      btn.appendChild(label);

      btn.addEventListener("click", function () {
        currentStage = idx;
        currentIteration = deriveIterationForStage(q, idx);
        nav.querySelectorAll(".pipeline-btn").forEach(function (b, i) {
          b.classList.toggle("active", i === idx);
        });
        renderStageContent(q, idx);
        renderPerfChart(q);
        renderTimeline(q);
      });

      nav.appendChild(btn);
    });
  }

  // ── Stage Content (the big swap) ──
  function renderStageContent(q, stageIdx) {
    var container = $("stage-content");
    container.innerHTML = "";
    if (!q.pipeline || !q.pipeline[stageIdx]) return;

    var stage = q.pipeline[stageIdx];
    var stageName = stage.stage.toLowerCase();
    var isAnalysisStage = stageName.indexOf("analysis") !== -1;
    var isStorageStage = stageName.indexOf("storage") !== -1;
    var isBaseStage = stageName === "base";
    var isImplementationStage =
      stageName.indexOf("base optimization") !== -1 ||
      stageName.indexOf("human persona") !== -1 ||
      stageName.indexOf("optim") !== -1 ||
      stageName.indexOf("final") !== -1 ||
      stageName.indexOf("simd") !== -1 ||
      stageName.indexOf("memory") !== -1 ||
      stageName.indexOf("codegen") !== -1 ||
      stageName.indexOf("bitmap") !== -1 ||
      stageName.indexOf("single") !== -1 ||
      stageName.indexOf("full avx") !== -1;

    // Reasoning block (always shown)
    var reasoning = buildReasoningBlock(stage);
    container.appendChild(reasoning);

    // Stage-specific content
    if (isAnalysisStage) {
      renderAnalysisStage(container, q);
    } else if (isStorageStage) {
      renderStorageStage(container, q);
    } else if (isBaseStage) {
      renderCodeStage(container, q, 0);
    } else if (isImplementationStage) {
      renderCodeStage(container, q, deriveIterationForStage(q, stageIdx));
    } else {
      renderCodeStage(container, q, deriveIterationForStage(q, stageIdx));
    }
  }

  function buildReasoningBlock(stage) {
    var block = document.createElement("div");
    block.className = "stage-reasoning-block";

    var textEl = document.createElement("div");
    textEl.className = "stage-reasoning-text";
    textEl.textContent = stage.reasoning;

    if (stage.decisions && stage.decisions.length) {
      var decisions = document.createElement("div");
      decisions.className = "stage-decisions";
      stage.decisions.forEach(function (d) {
        var tag = document.createElement("span");
        tag.className = "stage-decision-tag";
        tag.textContent = d;
        decisions.appendChild(tag);
      });
      textEl.appendChild(decisions);
    }

    block.appendChild(textEl);
    return block;
  }

  // ── Stage: Query Analysis ──
  function renderAnalysisStage(container, q) {
    var grid = document.createElement("div");
    grid.className = "stage-grid";

    // Left: SQL
    var sqlCard = createCard("SQL Query", q.name);
    sqlCard.classList.add("analysis-card", "analysis-card--sql");
    var sqlBody = document.createElement("div");
    sqlBody.className = "card-body";
    var sqlViewer = document.createElement("div");
    sqlViewer.className = "sql-viewer";
    var sqlPre = document.createElement("pre");
    sqlPre.innerHTML = highlightSQL(q.sql, q.placeholders);
    sqlViewer.appendChild(sqlPre);
    sqlBody.appendChild(sqlViewer);
    sqlCard.appendChild(sqlBody);
    grid.appendChild(sqlCard);

    // Right: Query Plan
    var planCard = createCard("Query Plan", ENABLE_BRANCHING_PLAN_VIEW ? "branching tree" : "operator flow");
    planCard.classList.add("analysis-card", "analysis-card--plan");
    var planBody = document.createElement("div");
    planBody.className = "card-body";
    if (q.queryPlan) {
      var tree = document.createElement("div");
      tree.className = "plan-tree " + (ENABLE_BRANCHING_PLAN_VIEW ? "plan-tree--branch" : "plan-tree--stack");
      tree.appendChild(buildPlanNode(tree, q.queryPlan, 0));
      planBody.appendChild(tree);
    }
    planCard.appendChild(planBody);
    grid.appendChild(planCard);

    container.appendChild(grid);
  }

  // ── Stage: Storage Design ──
  function renderStorageStage(container, q) {
    var stack = document.createElement("div");
    stack.className = "storage-full-stack";
    stack.appendChild(buildStorageMainCompareCard(q));
    stack.appendChild(buildStorageNarrativeTechniquesRow(q));
    stack.appendChild(buildStorageSqlCard(q));
    container.appendChild(stack);
  }

  // ── Stage: Code (Base + Optimizations) ──
  function renderCodeStage(container, q, iterIdx) {
    currentIteration = Math.min(iterIdx, q.iterations.length - 1);

    var codeCard = createCard("Generated C++", "");
    // Tabs
    var tabs = document.createElement("div");
    tabs.className = "code-tabs";
    q.iterations.forEach(function (iter, idx) {
      var btn = document.createElement("button");
      btn.className = "code-tab" + (idx === currentIteration ? " active" : "");
      btn.textContent = iter.label;
      btn.addEventListener("click", function () {
        currentIteration = idx;
        tabs.querySelectorAll(".code-tab").forEach(function (t, i) {
          t.classList.toggle("active", i === idx);
        });
        renderCodeInto(bodyEl, iter);
        renderPerfChart(q);
        renderTimeline(q);
      });
      tabs.appendChild(btn);
    });
    codeCard.appendChild(tabs);

    var bodyEl = document.createElement("div");
    bodyEl.className = "code-body";
    codeCard.appendChild(bodyEl);

    var descEl = document.createElement("div");
    descEl.className = "iter-desc";
    codeCard.descEl = descEl;
    codeCard.appendChild(descEl);

    renderCodeInto(bodyEl, q.iterations[currentIteration]);
    // Update desc
    var iter = q.iterations[currentIteration];
    var descHtml = '<span>' + escHtml(iter.description) + '</span>';
    if (iter.diff_summary) {
      descHtml += '<span class="diff-tag">' + escHtml(iter.diff_summary) + '</span>';
    }
    descEl.innerHTML = descHtml;

    container.appendChild(codeCard);
  }

  function renderCodeInto(bodyEl, iter) {
    var code = iter.code;
    var highlighted;
    if (typeof hljs !== "undefined") {
      highlighted = hljs.highlight(code, { language: "cpp" }).value;
    } else {
      highlighted = escHtml(code);
    }
    var lines = highlighted.split("\n");
    var html = lines.map(function (line, i) {
      return '<span class="line-num">' + (i + 1) + '</span>' + line;
    }).join("\n");
    bodyEl.innerHTML = '<pre>' + html + '</pre>';

    // Update desc if card has descEl
    var card = bodyEl.parentElement;
    if (card && card.descEl) {
      var descHtml = '<span>' + escHtml(iter.description) + '</span>';
      if (iter.diff_summary) {
        descHtml += '<span class="diff-tag">' + escHtml(iter.diff_summary) + '</span>';
      }
      card.descEl.innerHTML = descHtml;
    }
  }

  // ── Helpers ──
  function createCard(title, subtitle) {
    var card = document.createElement("div");
    card.className = "card";
    var header = document.createElement("div");
    header.className = "card-header";
    var titleEl = document.createElement("span");
    titleEl.textContent = title;
    header.appendChild(titleEl);
    if (subtitle) {
      var sub = document.createElement("span");
      sub.className = "card-subtitle";
      sub.textContent = subtitle;
      header.appendChild(sub);
    }
    card.appendChild(header);
    return card;
  }

  function buildPlanNode(treeRoot, node, depth) {
    var wrapper = document.createElement("div");
    wrapper.className = "plan-node";

    var row = document.createElement("div");
    row.className = "plan-node-row";

    var box = document.createElement("div");
    box.className = "plan-node-box";

    var top = document.createElement("div");
    top.className = "plan-node-top";

    var op = document.createElement("span");
    op.className = "plan-op";
    op.textContent = node.operator;
    top.appendChild(op);

    if (node.estRows !== undefined) {
      var rows = document.createElement("span");
      rows.className = "plan-rows";
      rows.textContent = formatRows(node.estRows) + " rows";
      top.appendChild(rows);
    }

    box.appendChild(top);

    if (node.detail) {
      var detail = document.createElement("div");
      detail.className = "plan-detail";
      detail.textContent = node.detail;
      box.appendChild(detail);
    }

    var meta = document.createElement("div");
    meta.className = "plan-meta";
    var hasMeta = false;

    if (node.joinType) {
      meta.appendChild(buildPlanMetaChip("Join", node.joinType));
      hasMeta = true;
    }
    if (node.selectivity !== undefined) {
      meta.appendChild(buildPlanMetaChip("Selectivity", (node.selectivity * 100).toFixed(1) + "%"));
      hasMeta = true;
    }
    if (node.columns && node.columns.length) {
      meta.appendChild(buildPlanMetaChip("Cols", node.columns.slice(0, 3).join(", ") + (node.columns.length > 3 ? " +" + (node.columns.length - 3) : "")));
      hasMeta = true;
    }

    if (hasMeta) box.appendChild(meta);
    row.appendChild(box);

    row.addEventListener("click", function (e) {
      e.stopPropagation();
      var was = row.classList.contains("highlight");
      treeRoot.querySelectorAll(".plan-node-row.highlight").forEach(function (r) {
        r.classList.remove("highlight");
      });
      if (!was) row.classList.add("highlight");
    });

    wrapper.appendChild(row);

    var children = node.children || (node.child ? [node.child] : []);
    if (children.length) {
      wrapper.classList.add("has-children");
      var childContainer = document.createElement("div");
      childContainer.className = "plan-children";
      children.forEach(function (child) {
        childContainer.appendChild(buildPlanNode(treeRoot, child, depth + 1));
      });
      wrapper.appendChild(childContainer);
    }
    return wrapper;
  }

  function buildPlanMetaChip(label, value) {
    var chip = document.createElement("span");
    chip.className = "plan-meta-chip";
    chip.innerHTML =
      '<span class="plan-meta-label">' + escHtml(label) + '</span>' +
      '<span class="plan-meta-value">' + escHtml(value) + '</span>';
    return chip;
  }

  function buildStorageColumns(design) {
    var cols = document.createElement("div");
    cols.className = "storage-columns";
    design.columns.forEach(function (col) {
      var row = document.createElement("div");
      row.className = "storage-col";

      var name = document.createElement("span");
      name.className = "storage-col-name";
      name.textContent = col.name;
      row.appendChild(name);

      var type = document.createElement("span");
      type.className = "storage-col-type";
      type.textContent = col.type;
      row.appendChild(type);

      var enc = document.createElement("span");
      var cls = col.encoding.toLowerCase().replace(/\s/g, "");
      if (cls === "dictionary") cls = "dict";
      enc.className = "storage-encoding " + cls;
      enc.textContent = col.encoding;
      row.appendChild(enc);

      if (col.sortKey) {
        var sort = document.createElement("span");
        sort.className = "storage-sort-badge";
        sort.textContent = "SORT";
        row.appendChild(sort);
      }
      if (col.cardinality) {
        var card = document.createElement("span");
        card.className = "storage-encoding raw";
        card.textContent = "|" + col.cardinality + "|";
        card.title = col.cardinality + " distinct values";
        row.appendChild(card);
      }
      if (col.reason) {
        var reason = document.createElement("span");
        reason.className = "storage-col-reason";
        reason.textContent = col.reason;
        row.appendChild(reason);
      }
      cols.appendChild(row);
    });
    return cols;
  }

  function buildStorageIndexes(design) {
    var container = document.createElement("div");
    container.className = "storage-indexes";
    design.indexes.forEach(function (idx) {
      var tag = document.createElement("span");
      tag.className = "storage-index-tag";
      tag.textContent = idx.type + ": " + idx.column;
      tag.title = idx.reason;
      container.appendChild(tag);
    });
    return container;
  }

  function buildStorageHero(q) {
    var hero = document.createElement("div");
    hero.className = "storage-hero";

    var stats = [
      { label: "Columns", value: q.storageDesign && q.storageDesign.columns ? q.storageDesign.columns.length : 0 },
      { label: "Indexes", value: q.storageDesign && q.storageDesign.indexes ? q.storageDesign.indexes.length : 0 },
      { label: "Sort Keys", value: countSortKeys(q.storageDesign) },
      { label: "Encodings", value: countUniqueEncodings(q.storageDesign) }
    ];

    stats.forEach(function (stat) {
      var chip = document.createElement("div");
      chip.className = "storage-stat";

      var value = document.createElement("strong");
      value.textContent = stat.value;
      chip.appendChild(value);

      var label = document.createElement("span");
      label.textContent = stat.label;
      chip.appendChild(label);

      hero.appendChild(chip);
    });

    return hero;
  }

  function buildStorageOverviewPanel(q) {
    var wrap = document.createElement("div");
    wrap.className = "storage-overview";

    var summary = document.createElement("div");
    summary.className = "storage-overview-main";

    var eyebrow = document.createElement("div");
    eyebrow.className = "storage-overview-eyebrow";
    eyebrow.textContent = "Storage Narrative";
    summary.appendChild(eyebrow);

    var title = document.createElement("h3");
    title.className = "storage-overview-title";
    title.textContent = currentQuery + " runs on a workload-shaped physical design";
    summary.appendChild(title);

    var text = document.createElement("p");
    text.className = "storage-overview-text";
    text.textContent = getOverviewNarrative(q);
    summary.appendChild(text);

    var context = getDuckdbQueryContext(currentQuery);
    var mix = getSelectedEncodingMix(context);
    if (mix.length) {
      summary.appendChild(buildEncodingMixChips(mix));
    }

    summary.appendChild(buildQueryPathRail(currentQuery));
    wrap.appendChild(summary);

    return wrap;
  }

  function buildStorageQueryInsightsCard(q) {
    var card = createCard("Query-Specific Insights", "why this layout accelerates " + currentQuery);
    var body = document.createElement("div");
    body.className = "card-body";

    var queryInsight = getQueryInsight(currentQuery);
    if (queryInsight) {
      body.appendChild(buildMiniStatRow([
        { label: "Query", value: currentQuery },
        { label: "Tables", value: queryInsight.tables_accessed.length },
        { label: "Core Ideas", value: queryInsight.key_optimizations.length },
        { label: "Bottleneck", value: shortBottleneckLabel(queryInsight.primary_bottleneck) }
      ]));
      body.appendChild(buildMechanismBoard(queryInsight));
    } else {
      body.appendChild(buildInsightSummary(buildFallbackQueryNarrative(q)));
      body.appendChild(buildMiniStatRow([
        { label: "Columns", value: q.storageDesign && q.storageDesign.columns ? q.storageDesign.columns.length : 0 },
        { label: "Indexes", value: q.storageDesign && q.storageDesign.indexes ? q.storageDesign.indexes.length : 0 },
        { label: "Stages", value: q.iterations.length }
      ]));
    }

    card.appendChild(body);
    return card;
  }

  function buildMechanismBoard(queryInsight) {
    var board = document.createElement("div");
    board.className = "mechanism-board";

    board.appendChild(buildMechanismTile("Pressure Point", queryInsight.primary_bottleneck, "bottleneck"));
    board.appendChild(buildMechanismTile("Main Driver", queryInsight.speedup_driver, "driver"));

    (queryInsight.key_optimizations || []).slice(0, 4).forEach(function (item) {
      board.appendChild(buildMechanismTile("Core Idea", item, "idea"));
    });

    return board;
  }

  function buildMechanismTile(label, value, kind) {
    var tile = document.createElement("div");
    tile.className = "mechanism-tile " + (kind || "idea");

    var k = document.createElement("span");
    k.className = "mechanism-label";
    k.textContent = label;
    tile.appendChild(k);

    var v = document.createElement("strong");
    v.className = "mechanism-value";
    v.textContent = value;
    tile.appendChild(v);

    return tile;
  }

  function buildQueryPathRail(qid) {
    var rail = document.createElement("div");
    rail.className = "query-path-rail";

    var insight = getQueryInsight(qid);
    var tables = insight && insight.tables_accessed && insight.tables_accessed.length
      ? insight.tables_accessed
      : inferTablesFromStorage(qid);

    tables.forEach(function (table, idx) {
      var node = document.createElement("div");
      node.className = "query-path-node";
      if (idx === 0) node.classList.add("primary");
      node.textContent = table;
      rail.appendChild(node);

      if (idx < tables.length - 1) {
        var arrow = document.createElement("div");
        arrow.className = "query-path-arrow";
        arrow.textContent = "->";
        rail.appendChild(arrow);
      }
    });

    return rail;
  }

  function buildTechniqueRibbon(qid) {
    var wrap = document.createElement("div");
    wrap.className = "technique-ribbon";

    var title = document.createElement("div");
    title.className = "technique-ribbon-title";
    title.textContent = "Top Techniques In Play";
    wrap.appendChild(title);

    var techniques = getRelevantTechniques(qid);
    if (!techniques.length && storageAnalysis && storageAnalysis.derived && storageAnalysis.derived.technique_leaderboard) {
      techniques = storageAnalysis.derived.technique_leaderboard.slice(0, 3);
    }
    if (!techniques.length) {
      techniques = buildFallbackTechniques(data.queries[qid]);
    }

    techniques.slice(0, 3).forEach(function (tech, idx) {
      var item = document.createElement("div");
      item.className = "technique-ribbon-item";

      var rank = document.createElement("div");
      rank.className = "technique-ribbon-rank";
      rank.textContent = "#" + (idx + 1);
      item.appendChild(rank);

      var body = document.createElement("div");
      body.className = "technique-ribbon-body";
      body.innerHTML =
        '<strong>' + escHtml(tech.name) + '</strong>' +
        '<span>' + escHtml((tech.category || "storage") + " • " + (tech.table || "query-local")) + '</span>';
      item.appendChild(body);

      wrap.appendChild(item);
    });

    return wrap;
  }

  function buildStorageGlobalHighlightsCard(q) {
    var card = createCard("Storage Highlights", "what makes this physical design special");
    var body = document.createElement("div");
    body.className = "card-body";

    var list = document.createElement("div");
    list.className = "storage-highlight-grid";
    var cards = getRelevantHighlightCards(currentQuery);

    if (!cards.length) {
      cards = buildFallbackHighlightCards(q);
    }

    cards.slice(0, 4).forEach(function (item) {
      var block = document.createElement("div");
      block.className = "storage-highlight-card compact";

      var title = document.createElement("div");
      title.className = "storage-highlight-title";
      title.textContent = item.title;
      block.appendChild(title);

      var value = document.createElement("div");
      value.className = "storage-highlight-value";
      value.textContent = prettifyInsightValue(item.title, item.value);
      block.appendChild(value);

      list.appendChild(block);
    });

    body.appendChild(list);

    var coverage = getCoverageSummary();
    if (coverage) {
      body.appendChild(buildMiniStatRow([
        { label: "Covered", value: coverage.total_queries_covered + "/22" },
        { label: "Coverage", value: coverage.coverage_pct + "%" },
        { label: "Best-Supported", value: (coverage.most_supported_queries || []).slice(0, 2).join(", ") || "n/a" }
      ]));
    }

    card.appendChild(body);
    return card;
  }

  function buildStorageTechniquesCard(q) {
    var card = createCard("Relevant Techniques", "specialized mechanisms behind this query");
    var body = document.createElement("div");
    body.className = "card-body";

    var techniques = getRelevantTechniques(currentQuery);
    if (!techniques.length) {
      techniques = buildFallbackTechniques(q);
    }

    var list = document.createElement("div");
    list.className = "technique-list";

    techniques.forEach(function (tech) {
      var item = document.createElement("div");
      item.className = "technique-item";

      var top = document.createElement("div");
      top.className = "technique-topline";

      var name = document.createElement("span");
      name.className = "technique-name";
      name.textContent = tech.name;
      top.appendChild(name);

      var badge = document.createElement("span");
      badge.className = "technique-badge";
      badge.textContent = friendlyTechniqueBadge(tech);
      top.appendChild(badge);

      item.appendChild(top);

      var meta = document.createElement("div");
      meta.className = "technique-meta";
      meta.textContent = (tech.table || "query-local") + " • " + (tech.category || "storage");
      item.appendChild(meta);

      var desc = document.createElement("div");
      desc.className = "technique-desc";
      desc.textContent = tech.why_it_matters || tech.why_its_novel || tech.reason || tech.detail || "Specialized layout choice for this query.";
      item.appendChild(desc);

      list.appendChild(item);
    });

    body.appendChild(list);
    card.appendChild(body);
    return card;
  }

  function buildStorageBlueprintCard(q) {
    var card = createCard("Physical Blueprint", "how the data is laid out on disk");
    var body = document.createElement("div");
    body.className = "card-body";

    var blueprint = getPrimaryTableBlueprint(currentQuery);
    if (!blueprint) {
      body.appendChild(buildInsightSummary("The per-query storage design already shows the tuned columns, but the richer global table blueprint is missing."));
      card.appendChild(body);
      return card;
    }

    body.appendChild(buildMiniStatRow([
      { label: "Row Layout", value: humanizeToken(blueprint.row_layout) },
      { label: "Indexes", value: blueprint.indexes ? blueprint.indexes.length : 0 },
      { label: "Encoded Columns", value: blueprint.column_encodings ? blueprint.column_encodings.length : 0 }
    ]));

    body.appendChild(buildEncodingMixViz(blueprint));

    if (blueprint.partitioning || (blueprint.sort_order && blueprint.sort_order.columns)) {
      var layoutFacts = [];
      if (blueprint.partitioning) layoutFacts.push("Partitioning: " + blueprint.partitioning);
      if (blueprint.sort_order && blueprint.sort_order.columns) {
        layoutFacts.push("Sort Order: " + blueprint.sort_order.columns.join(" -> "));
      }
      body.appendChild(buildTokenSection("Layout Decisions", layoutFacts, "muted"));
    }

    if (blueprint.notable_features && blueprint.notable_features.length) {
      body.appendChild(buildTokenSection("Standout Physical Choices", blueprint.notable_features.slice(0, 4), "accent"));
    }

    card.appendChild(body);
    return card;
  }

  function buildStorageMainCompareCard(q) {
    var card = createCard("How We Store It Instead", "One Storage for ALL Queries!");
    card.classList.add("storage-showcase-card");
    var body = document.createElement("div");
    body.className = "card-body";

    var context = getDuckdbQueryContext(currentQuery);
    if (!context) {
      body.appendChild(buildInsightSummary("Run the DuckDB context helper and this section will show how real sampled values are transformed into the bespoke storage representation."));
      card.appendChild(body);
      return card;
    }

    body.appendChild(buildStorageTableSelector(q, context));
    var compare = buildStorageRowCompareShowcase(context);
    if (compare) {
      body.appendChild(compare);
    } else {
      body.appendChild(buildInsightSummary("No transformed row preview is available yet for this query."));
    }

    card.appendChild(body);
    return card;
  }

  function buildStorageNarrativeTechniquesRow(q) {
    var row = document.createElement("div");
    row.className = "storage-narrative-row";
    row.appendChild(buildStorageOverviewPanel(q));
    row.appendChild(buildStorageTechniqueBoxesCard(q));
    return row;
  }

  function buildStorageTechniqueBoxesCard(q) {
    var card = createCard("Top Techniques", "the key storage moves for this query");
    card.classList.add("storage-technique-boxes-card");
    var body = document.createElement("div");
    body.className = "card-body";

    var boxes = document.createElement("div");
    boxes.className = "storage-technique-boxes";

    var items = getStorageTechniqueBoxItems(q);
    if (!items.length) {
      body.appendChild(buildInsightSummary("No compact storage techniques are available yet for this query."));
      card.appendChild(body);
      return card;
    }

    items.forEach(function (item) {
      var box = document.createElement("div");
      box.className = "storage-technique-box";

      var title = document.createElement("div");
      title.className = "storage-technique-box-title";
      title.textContent = item.title;
      box.appendChild(title);

      if (item.copy) {
        var copy = document.createElement("div");
        copy.className = "storage-technique-box-copy";
        copy.textContent = item.copy;
        box.appendChild(copy);
      }

      boxes.appendChild(box);
    });

    body.appendChild(boxes);
    card.appendChild(body);
    return card;
  }

  function getStorageTechniqueBoxItems(q) {
    var stage = getStorageStage(q);
    var items = [];
    if (stage && stage.decisions && stage.decisions.length) {
      stage.decisions.slice(0, 4).forEach(function (decision) {
        items.push(parseStorageDecision(decision));
      });
    }
    if (items.length) return items;

    var techniques = getRelevantTechniques(currentQuery).slice(0, 3);
    return techniques.map(function (tech) {
      return {
        title: getTechniqueLabel(tech),
        copy: getTechniqueBullet(currentQuery, tech)
      };
    });
  }

  function getStorageStage(q) {
    if (!q || !q.pipeline) return null;
    return q.pipeline.find(function (stage) {
      return String(stage.stage || "").toLowerCase().indexOf("storage") !== -1;
    }) || null;
  }

  function parseStorageDecision(decision) {
    var raw = String(decision || "").trim();
    if (!raw) {
      return { title: "Storage choice", copy: "" };
    }
    var parts = raw.split(":");
    if (parts.length > 1) {
      return {
        title: parts[0].trim(),
        copy: parts.slice(1).join(":").trim()
      };
    }

    var arrowParts = raw.split("→");
    if (arrowParts.length > 1) {
      return {
        title: arrowParts[0].trim(),
        copy: arrowParts.slice(1).join("→").trim()
      };
    }

    return {
      title: raw,
      copy: ""
    };
  }

  function buildStorageLayoutCard(q) {
    var card = createCard("Bespoke Physical Layout", "the storage choices we commit to for this query");
    var body = document.createElement("div");
    body.className = "card-body";
    if (q.storageDesign) {
      body.appendChild(buildStorageHero(q));
      body.appendChild(buildStorageColumns(q.storageDesign));
      if (q.storageDesign.indexes && q.storageDesign.indexes.length) {
        body.appendChild(buildStorageIndexes(q.storageDesign));
      }
      if (q.storageDesign.insight) {
        var insight = document.createElement("div");
        insight.className = "storage-insight";
        insight.textContent = q.storageDesign.insight;
        body.appendChild(insight);
      }
    } else {
      body.appendChild(buildInsightSummary("This query does not currently have an explicit storage design block in the demo data."));
    }
    card.appendChild(body);
    return card;
  }

  function buildStorageSupportCard(q) {
    var card = createCard("Why It Helps", "a compact summary of the key mechanisms");
    var body = document.createElement("div");
    body.className = "card-body";

    var grid = document.createElement("div");
    grid.className = "storage-support-grid";

    var insight = getQueryInsight(currentQuery);
    if (insight) {
      grid.appendChild(buildCompactSupportPanel(
        "Query Path",
        (insight.tables_accessed || inferTablesFromStorage(currentQuery)).join(" -> ") || "table-local"
      ));
    }

    var techniques = getRelevantTechniques(currentQuery);
    if (!techniques.length) techniques = buildFallbackTechniques(q);
    if (techniques.length) {
      grid.appendChild(buildCompactSupportPanel(
        "Top Mechanisms",
        techniques.slice(0, 3).map(function (tech) { return tech.name; }).join(" • ")
      ));
    }

    var blueprint = getPrimaryTableBlueprint(currentQuery);
    if (blueprint) {
      var facts = [];
      if (blueprint.partitioning) facts.push("Partitioned");
      if (blueprint.row_layout) facts.push(humanizeToken(blueprint.row_layout));
      if (blueprint.indexes && blueprint.indexes.length) facts.push(blueprint.indexes.length + " indexes");
      if (blueprint.column_encodings && blueprint.column_encodings.length) facts.push(blueprint.column_encodings.length + " encoded cols");
      grid.appendChild(buildCompactSupportPanel("Physical Shape", facts.join(" • ")));
    }

    body.appendChild(grid);
    card.appendChild(body);
    return card;
  }

  function buildStorageSqlCard(q) {
    var card = createCard("SQL Context", q.name);
    var body = document.createElement("div");
    body.className = "card-body";
    var sqlViewer = document.createElement("div");
    sqlViewer.className = "sql-viewer";
    var sqlPre = document.createElement("pre");
    sqlPre.innerHTML = highlightSQL(q.sql, q.placeholders);
    sqlViewer.appendChild(sqlPre);
    body.appendChild(sqlViewer);
    card.appendChild(body);
    return card;
  }

  function buildStorageMessageStrip(q, context) {
    var strip = document.createElement("div");
    strip.className = "storage-message-strip";

    var intro = document.createElement("div");
    intro.className = "storage-message-main";
    intro.textContent = (q.storageDesign && q.storageDesign.insight)
      || "We keep the same information, but store it in shapes that match the dominant predicates, joins, and arithmetic.";
    strip.appendChild(intro);

    var contextBits = [];
    if (context.tables && context.tables.length) contextBits.push("tables: " + context.tables.join(", "));
    if (context.layout_preview && context.layout_preview.length) {
      contextBits.push("transformed columns: " + context.layout_preview.slice(0, 4).map(function (item) { return item.column_name; }).join(", "));
    }
    if (context.result_preview && context.result_preview.rows && context.result_preview.rows.length) {
      contextBits.push("result preview: " + context.result_preview.rows.length + " rows");
    }

    if (contextBits.length) {
      var meta = document.createElement("div");
      meta.className = "storage-message-meta";
      meta.textContent = contextBits.join("  •  ");
      strip.appendChild(meta);
    }

    return strip;
  }

  function buildStorageActualDataBlock(context) {
    var section = document.createElement("div");
    section.className = "storage-showcase-section";

    var title = document.createElement("div");
    title.className = "storage-showcase-title";
    title.textContent = "Original Data";
    section.appendChild(title);

    var subtitle = document.createElement("div");
    subtitle.className = "storage-showcase-subtitle";
    subtitle.textContent = "These are the real source rows DuckDB sees before any bespoke physical reorganization.";
    section.appendChild(subtitle);

    var tablesWrap = document.createElement("div");
    tablesWrap.className = "storage-data-stack";

    Object.keys(context.table_samples || {}).slice(0, 2).forEach(function (tableName) {
      var block = document.createElement("div");
      block.className = "data-sample-section wide";

      var label = document.createElement("div");
      label.className = "data-sample-title";
      label.textContent = tableName.toUpperCase();
      block.appendChild(label);
      block.appendChild(buildMiniTable(context.table_samples[tableName]));
      tablesWrap.appendChild(block);
    });

    if (!tablesWrap.childNodes.length) {
      tablesWrap.appendChild(buildInsightSummary("No sampled source rows are available yet for this query."));
    }

    section.appendChild(tablesWrap);
    return section;
  }

  function buildStorageResultPreviewBlock(context) {
    var section = document.createElement("div");
    section.className = "storage-showcase-section";

    var title = document.createElement("div");
    title.className = "storage-showcase-title";
    title.textContent = "What The Query Produces";
    section.appendChild(title);

    var subtitle = document.createElement("div");
    subtitle.className = "storage-showcase-subtitle";
    subtitle.textContent = "A small result preview ties the physical design back to the final analytical output.";
    section.appendChild(subtitle);

    if (context.result_preview && context.result_preview.columns && context.result_preview.columns.length) {
      section.appendChild(buildMiniTable(context.result_preview));
    } else {
      section.appendChild(buildInsightSummary("No query result preview is available yet for this query."));
    }

    return section;
  }

  function buildStorageLayoutPrinciplesBlock(context) {
    var section = document.createElement("div");
    section.className = "storage-showcase-section";

    var title = document.createElement("div");
    title.className = "storage-showcase-title";
    title.textContent = "How We Store It Instead";
    section.appendChild(title);

    var subtitle = document.createElement("div");
    subtitle.className = "storage-showcase-subtitle storage-showcase-callout";
    subtitle.textContent = "One Storage for ALL Queries!";
    section.appendChild(subtitle);

    var list = document.createElement("div");
    list.className = "layout-preview-list";
    (context.layout_preview || []).slice(0, 3).forEach(function (item) {
      var row = document.createElement("div");
      row.className = "layout-preview-row compact";

      var head = document.createElement("div");
      head.className = "layout-preview-head";
      head.innerHTML =
        '<span class="layout-preview-name">' + escHtml(item.column_name) + '</span>' +
        '<span class="layout-preview-encoding">' + escHtml(humanizeToken(item.encoding)) + '</span>';
      row.appendChild(head);

      var payoff = document.createElement("div");
      payoff.className = "layout-preview-payoff";
      payoff.innerHTML =
        '<span class="layout-preview-arrow">→</span>' +
        '<span class="layout-preview-payoff-copy">' + escHtml(encodingBenefitText(item)) + '</span>';
      row.appendChild(payoff);

      list.appendChild(row);
    });
    section.appendChild(list);
    return section;
  }

  function buildStorageRowCompareShowcase(context) {
    var selectedTable = getSelectedStorageTable(context);
    var tablePreviews = context.table_layout_previews || {};
    var items = (tablePreviews[selectedTable] || context.layout_preview || []).slice(0, 5);
    if (!items.length) return null;

    var rowCount = items.reduce(function (best, item) {
      return Math.max(best, (item.raw_examples || []).length, (item.encoded_examples || []).length);
    }, 0);
    if (!rowCount) return null;

    var wrap = document.createElement("div");
    wrap.className = "storage-row-showcase";
    wrap.appendChild(buildStoragePairedCompareTable(
      "Raw vs Bespoke Storage",
      "Each source column is immediately followed by the version we store. Green columns are storage-shaped representations.",
      items,
      rowCount
    ));

    return wrap;
  }

  function buildStoragePairedCompareTable(title, subtitle, items, rowCount) {
    var block = document.createElement("div");
    block.className = "storage-compare-block paired";

    var head = document.createElement("div");
    head.className = "storage-compare-head";
    head.innerHTML =
      '<div class="storage-compare-title">' + escHtml(title) + '</div>' +
      '<div class="storage-compare-copy">' + escHtml(subtitle) + '</div>';
    block.appendChild(head);

    var wrap = document.createElement("div");
    wrap.className = "storage-compare-wrap";
    var table = document.createElement("table");
    table.className = "storage-compare-table paired";

    var thead = document.createElement("thead");
    var tr = document.createElement("tr");
    items.forEach(function (item) {
      var rawTh = document.createElement("th");
      rawTh.className = "storage-compare-th raw";
      rawTh.innerHTML =
        '<div class="storage-compare-th-title">' + escHtml(item.column_name) + '</div>' +
        '<div class="storage-compare-th-meta">original value</div>';
      tr.appendChild(rawTh);

      var meta = storageColumnMeta(item);
      var storedTh = document.createElement("th");
      storedTh.className = "storage-compare-th stored";
      storedTh.innerHTML =
        '<div class="storage-compare-th-title">' + escHtml(meta.title) + '</div>' +
        '<div class="storage-compare-th-meta">' + escHtml(meta.subtitle) +
        (meta.isNew ? '<span class="storage-new-badge">Bespoke</span>' : '') +
        '</div>';
      tr.appendChild(storedTh);
    });
    thead.appendChild(tr);
    table.appendChild(thead);

    var tbody = document.createElement("tbody");
    for (var rowIdx = 0; rowIdx < rowCount; rowIdx++) {
      var row = document.createElement("tr");
      items.forEach(function (item) {
        var rawTd = document.createElement("td");
        rawTd.className = "storage-compare-td raw";
        rawTd.textContent = (item.raw_examples || [])[rowIdx] != null ? String(item.raw_examples[rowIdx]) : "";
        row.appendChild(rawTd);

        var storedTd = document.createElement("td");
        storedTd.className = "storage-compare-td stored";
        storedTd.textContent = (item.encoded_examples || [])[rowIdx] != null ? String(item.encoded_examples[rowIdx]) : "";
        row.appendChild(storedTd);
      });
      tbody.appendChild(row);
    }
    table.appendChild(tbody);
    wrap.appendChild(table);
    block.appendChild(wrap);

    return block;
  }

  function storageColumnMeta(item) {
    var suffixByEncoding = {
      dictionary: "_dict_id",
      day_offset: "_day_offset",
      delta: "_delta",
      front_coding: "_frontcoded",
      rle: "_runs",
      bitpacked: "_bits",
      scaled_int: "_scaled",
      micro_aos: "_packed"
    };
    var title = item.column_name;
    var isNew = false;
    if (item.encoding && item.encoding !== "raw" && suffixByEncoding[item.encoding]) {
      title = item.column_name + suffixByEncoding[item.encoding];
      isNew = true;
    }
    return {
      title: title,
      subtitle: humanizeToken(item.encoding),
      isNew: isNew
    };
  }

  function buildStorageTableSelector(q, context) {
    var tables = getAvailableStorageTables(context);
    if (tables.length <= 1) return document.createElement("div");

    var wrap = document.createElement("div");
    wrap.className = "storage-table-selector";

    var label = document.createElement("div");
    label.className = "storage-table-selector-label";
    label.textContent = "Preview Table";
    wrap.appendChild(label);

    var chips = document.createElement("div");
    chips.className = "storage-table-selector-chips";
    var activeTable = getSelectedStorageTable(context);

    tables.forEach(function (tableName) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "storage-table-chip" + (tableName === activeTable ? " active" : "");
      btn.textContent = tableName.toUpperCase();
      btn.addEventListener("click", function () {
        currentStorageTable = tableName;
        renderStageContent(q, currentStage);
      });
      chips.appendChild(btn);
    });

    wrap.appendChild(chips);
    return wrap;
  }

  function getSelectedStorageTable(context) {
    var tables = getAvailableStorageTables(context);
    if (currentStorageTable && tables.indexOf(currentStorageTable) !== -1) return currentStorageTable;
    if (context.focus_table && tables.indexOf(context.focus_table) !== -1) return context.focus_table;
    return tables[0] || null;
  }

  function getAvailableStorageTables(context) {
    if (!context) return [];
    var tables = (context.tables || []).slice();
    Object.keys(context.table_samples || {}).forEach(function (table) {
      if (tables.indexOf(table) === -1) tables.push(table);
    });
    Object.keys(context.table_layout_previews || {}).forEach(function (table) {
      if (tables.indexOf(table) === -1) tables.push(table);
    });
    return tables;
  }

  function buildCompactSupportPanel(title, text) {
    var panel = document.createElement("div");
    panel.className = "storage-support-panel";
    panel.innerHTML =
      '<div class="storage-support-title">' + escHtml(title) + '</div>' +
      '<div class="storage-support-copy">' + escHtml(text) + '</div>';
    return panel;
  }

  function buildStorageDataSamplesCard(q) {
    var card = createCard("Actual Data Glimpse", "a few source rows behind this query");
    var body = document.createElement("div");
    body.className = "card-body";

    var context = getDuckdbQueryContext(currentQuery);
    if (!context || !context.table_samples) {
      body.appendChild(buildInsightSummary("Once the DuckDB context helper has been run, this card shows real sampled rows from the source tables used by the query."));
      card.appendChild(body);
      return card;
    }

    Object.keys(context.table_samples).slice(0, 2).forEach(function (tableName) {
      var section = document.createElement("div");
      section.className = "data-sample-section";

      var label = document.createElement("div");
      label.className = "data-sample-title";
      label.textContent = tableName.toUpperCase();
      section.appendChild(label);

      section.appendChild(buildMiniTable(context.table_samples[tableName]));
      body.appendChild(section);
    });

    card.appendChild(body);
    return card;
  }

  function buildStorageLayoutPreviewCard(q) {
    var card = createCard("Why This Layout Wins", "from ordinary rows to workload-shaped storage");
    var body = document.createElement("div");
    body.className = "card-body";

    var context = getDuckdbQueryContext(currentQuery);
    if (!context || !context.layout_preview || !context.layout_preview.length) {
      body.appendChild(buildInsightSummary("Run the DuckDB context helper and this card will compare raw sampled values with the storage-oriented representation used in the bespoke layout."));
      card.appendChild(body);
      return card;
    }

    var list = document.createElement("div");
    list.className = "layout-preview-list";
    context.layout_preview.slice(0, 4).forEach(function (item) {
      var row = document.createElement("div");
      row.className = "layout-preview-row";

      var head = document.createElement("div");
      head.className = "layout-preview-head";
      head.innerHTML =
        '<span class="layout-preview-name">' + escHtml(item.column_name) + '</span>' +
        '<span class="layout-preview-encoding">' + escHtml(humanizeToken(item.encoding)) + '</span>';
      row.appendChild(head);

      var compare = document.createElement("div");
      compare.className = "layout-preview-compare";
      compare.appendChild(buildValueStack("Original Table", item.raw_examples, "raw"));
      compare.appendChild(buildValueStack("Bespoke Layout", item.encoded_examples, "stored"));
      row.appendChild(compare);

      var payoff = document.createElement("div");
      payoff.className = "layout-preview-payoff";
      payoff.innerHTML =
        '<span class="layout-preview-arrow">→</span>' +
        '<span class="layout-preview-payoff-copy">' + escHtml(encodingBenefitText(item)) + '</span>';
      row.appendChild(payoff);

      list.appendChild(row);
    });
    body.appendChild(list);

    card.appendChild(body);
    return card;
  }

  function buildMiniTable(preview) {
    var wrap = document.createElement("div");
    wrap.className = "mini-table-wrap";
    var table = document.createElement("table");
    table.className = "mini-table";

    var thead = document.createElement("thead");
    var tr = document.createElement("tr");
    (preview.columns || []).forEach(function (col) {
      var th = document.createElement("th");
      th.textContent = col;
      tr.appendChild(th);
    });
    thead.appendChild(tr);
    table.appendChild(thead);

    var tbody = document.createElement("tbody");
    (preview.rows || []).forEach(function (rowVals) {
      var row = document.createElement("tr");
      rowVals.forEach(function (val) {
        var td = document.createElement("td");
        td.textContent = val;
        row.appendChild(td);
      });
      tbody.appendChild(row);
    });
    table.appendChild(tbody);
    wrap.appendChild(table);
    return wrap;
  }

  function buildValueStack(title, values, tone) {
    var wrap = document.createElement("div");
    wrap.className = "value-stack" + (tone ? " " + tone : "");

    var label = document.createElement("div");
    label.className = "value-stack-label";
    label.textContent = title;
    wrap.appendChild(label);

    var list = document.createElement("div");
    list.className = "value-pill-list";
    (values || []).slice(0, 5).forEach(function (value) {
      var pill = document.createElement("span");
      pill.className = "value-pill";
      pill.textContent = value;
      list.appendChild(pill);
    });
    wrap.appendChild(list);

    return wrap;
  }

  function buildLayoutTransformationHero(context) {
    var wrap = document.createElement("div");
    wrap.className = "layout-transform-hero";

    var rawCard = document.createElement("div");
    rawCard.className = "layout-transform-card raw";
    rawCard.innerHTML =
      '<div class="layout-transform-eyebrow">Before</div>' +
      '<div class="layout-transform-title">Ordinary row-oriented values</div>' +
      '<div class="layout-transform-copy">Readable, but each scan keeps revisiting full-width values and generic layouts.</div>';
    wrap.appendChild(rawCard);

    var arrow = document.createElement("div");
    arrow.className = "layout-transform-arrow";
    arrow.textContent = "→";
    wrap.appendChild(arrow);

    var storedCard = document.createElement("div");
    storedCard.className = "layout-transform-card stored";
    storedCard.innerHTML =
      '<div class="layout-transform-eyebrow">After</div>' +
      '<div class="layout-transform-title">Bespoke query-shaped representation</div>' +
      '<div class="layout-transform-copy">The same facts are reorganized into tighter codes, offsets, and grouped fields that match the hot access path.</div>';
    wrap.appendChild(storedCard);

    if (context.encoding_mix && context.encoding_mix.length) {
      var strip = document.createElement("div");
      strip.className = "layout-transform-strip";
      context.encoding_mix.slice(0, 4).forEach(function (entry) {
        var chip = document.createElement("span");
        chip.className = "layout-transform-chip";
        chip.textContent = humanizeToken(entry.encoding) + " × " + entry.count;
        strip.appendChild(chip);
      });
      storedCard.appendChild(strip);
    }

    return wrap;
  }

  function encodingBenefitText(item) {
    var map = {
      dictionary: "Repeating values collapse into tiny ids, which makes filters and group-bys much lighter.",
      "day_offset": "Dates shrink into compact offsets, so temporal predicates compare cheap integers instead of full date objects.",
      delta: "Neighboring values turn into short increments, which compresses well when keys move monotonically.",
      "front_coding": "Shared string prefixes are stored once, keeping long text-like fields compact without losing fidelity.",
      rle: "Runs of the same value become countable streaks, ideal when a column has repeated modes or flags.",
      bitpacked: "Small integers are squeezed into fewer bits, which keeps hot columns dense in cache.",
      "scaled_int": "Decimals become integer math, which is cheaper to store and friendlier for SIMD-heavy arithmetic.",
      "micro_aos": "The hottest numeric fields travel together, so one fetch brings in the values the revenue math needs."
    };
    if (map[item.encoding]) return map[item.encoding];
    if (item.note) return item.note;
    return "The layout is tuned to the dominant access pattern so scans touch less and compute on simpler representations.";
  }

  function buildEncodingMixViz(blueprint) {
    var counts = {};
    (blueprint.column_encodings || []).forEach(function (col) {
      counts[col.encoding] = (counts[col.encoding] || 0) + 1;
    });
    var entries = Object.keys(counts).map(function (key) {
      return { key: key, count: counts[key] };
    }).sort(function (a, b) {
      return b.count - a.count;
    });

    var wrap = document.createElement("div");
    wrap.className = "encoding-mix";

    var bar = document.createElement("div");
    bar.className = "encoding-mix-bar";
    var total = entries.reduce(function (sum, item) { return sum + item.count; }, 0) || 1;

    entries.forEach(function (item) {
      var seg = document.createElement("div");
      seg.className = "encoding-mix-seg " + sanitizeClass(item.key);
      seg.style.width = ((item.count / total) * 100).toFixed(2) + "%";
      seg.title = humanizeToken(item.key) + ": " + item.count;
      bar.appendChild(seg);
    });

    wrap.appendChild(bar);

    var legend = document.createElement("div");
    legend.className = "encoding-mix-legend";
    entries.slice(0, 6).forEach(function (item) {
      var row = document.createElement("div");
      row.className = "encoding-legend-item";
      row.innerHTML =
        '<span class="encoding-dot ' + sanitizeClass(item.key) + '"></span>' +
        '<span class="encoding-legend-name">' + escHtml(humanizeToken(item.key)) + '</span>' +
        '<span class="encoding-legend-count">' + item.count + '</span>';
      legend.appendChild(row);
    });
    wrap.appendChild(legend);

    return wrap;
  }

  function buildCrossTableSystemsCard(q) {
    var card = createCard("Cross-Table Machinery", "the structures that tie the engine together");
    var body = document.createElement("div");
    body.className = "card-body";

    var systems = getRelevantCrossTableOptimizations(currentQuery);
    if (!systems.length) {
      body.appendChild(buildInsightSummary("This query is mostly explained by local table layout, so there are no major cross-table systems highlighted here."));
      card.appendChild(body);
      return card;
    }

    var list = document.createElement("div");
    list.className = "cross-system-list";
    systems.forEach(function (system) {
      var item = document.createElement("div");
      item.className = "cross-system-item";
      item.innerHTML =
        '<div class="cross-system-name">' + escHtml(system.name) + '</div>' +
        '<div class="cross-system-meta">' + escHtml((system.tables_involved || []).join(" -> ")) + '</div>' +
        '<div class="cross-system-desc">' + escHtml(system.mechanism) + '</div>' +
        '<div class="cross-system-benefit">' + escHtml(system.benefit) + '</div>';
      list.appendChild(item);
    });

    body.appendChild(list);
    card.appendChild(body);
    return card;
  }

  function buildStorageTableLeverageCard(q) {
    var card = createCard("Table Leverage", "where the storage plan spends its effort");
    var body = document.createElement("div");
    body.className = "card-body";

    var tableInsights = getRelevantTableInsights(currentQuery);
    if (tableInsights.length) {
      var list = document.createElement("div");
      list.className = "table-leverage-list";
      tableInsights.forEach(function (item) {
        var row = document.createElement("div");
        row.className = "table-leverage-row";

        var head = document.createElement("div");
        head.className = "table-leverage-head";
        head.innerHTML = '<span class="table-leverage-name">' + escHtml(item.table_name) + '</span>' +
          '<span class="table-leverage-score">' + qualitativeLeverageLabel(item.impact_score) + '</span>';
        row.appendChild(head);

        var track = document.createElement("div");
        track.className = "table-leverage-track";
        var fill = document.createElement("div");
        fill.className = "table-leverage-fill";
        fill.style.width = Math.max(8, item.impact_score) + "%";
        track.appendChild(fill);
        row.appendChild(track);

        var meta = document.createElement("div");
        meta.className = "table-leverage-meta";
        meta.textContent = item.query_count + " queries • " + item.technique_count + " techniques • " + item.index_count + " indexes";
        row.appendChild(meta);

        if (item.standout_features && item.standout_features.length) {
          row.appendChild(buildTokenSection("Standout Features", item.standout_features.slice(0, 3), "muted"));
        }

        list.appendChild(row);
      });
      body.appendChild(list);
    } else {
      body.appendChild(buildInsightSummary("This query already shows a bespoke column layout, but the global storage-plan analysis file has not been generated yet."));
    }

    var risks = getRelevantRisks(currentQuery);
    if (risks.length) {
      var riskWrap = document.createElement("div");
      riskWrap.className = "risk-list";
      risks.forEach(function (risk) {
        var riskEl = document.createElement("div");
        riskEl.className = "risk-item " + risk.severity;
        riskEl.innerHTML = '<strong>' + escHtml(risk.title) + '</strong><span>' + escHtml(risk.rationale) + '</span>';
        riskWrap.appendChild(riskEl);
      });
      body.appendChild(riskWrap);
    }

    card.appendChild(body);
    return card;
  }

  function buildInsightSummary(text) {
    var summary = document.createElement("div");
    summary.className = "storage-summary";
    summary.textContent = text;
    return summary;
  }

  function buildMiniStatRow(items) {
    var row = document.createElement("div");
    row.className = "storage-mini-stats";
    items.forEach(function (item) {
      var stat = document.createElement("div");
      stat.className = "storage-mini-stat";
      stat.innerHTML = '<strong>' + escHtml(String(item.value)) + '</strong><span>' + escHtml(item.label) + '</span>';
      row.appendChild(stat);
    });
    return row;
  }

  function buildLabelValue(label, value) {
    var block = document.createElement("div");
    block.className = "storage-label-block";
    block.innerHTML = '<span class="storage-label">' + escHtml(label) + '</span><span class="storage-value">' + escHtml(value) + '</span>';
    return block;
  }

  function buildTokenSection(title, values, tone) {
    var wrap = document.createElement("div");
    wrap.className = "token-section";

    var heading = document.createElement("div");
    heading.className = "token-section-title";
    heading.textContent = title;
    wrap.appendChild(heading);

    var list = document.createElement("div");
    list.className = "token-list";
    values.forEach(function (value) {
      var token = document.createElement("span");
      token.className = "token " + (tone || "accent");
      token.textContent = value;
      list.appendChild(token);
    });
    wrap.appendChild(list);

    return wrap;
  }

  function buildTagList(values, kind) {
    return buildTokenSection(kind === "table" ? "Tables Involved" : "Tags", values, "muted");
  }

  function getCoverageSummary() {
    return storageAnalysis && storageAnalysis.derived ? storageAnalysis.derived.coverage : null;
  }

  function getDuckdbQueryContext(qid) {
    return duckdbContext && duckdbContext.queries ? duckdbContext.queries[qid] : null;
  }

  function getAutomaticSummary() {
    return storageAnalysis && storageAnalysis.derived ? storageAnalysis.derived.automatic_summary : null;
  }

  function getQueryInsight(qid) {
    if (!storageAnalysis || !storageAnalysis.derived || !storageAnalysis.derived.query_insights) return null;
    return storageAnalysis.derived.query_insights.find(function (item) {
      return item.query_id === qid;
    }) || null;
  }

  function getQueryTechniqueHighlights(qid) {
    if (!storageAnalysis || !storageAnalysis.derived || !storageAnalysis.derived.query_technique_highlights) return [];
    return storageAnalysis.derived.query_technique_highlights.filter(function (item) {
      return item.query_id === qid;
    });
  }

  function getStorageHighlightMap(qid) {
    var map = {};
    var queryInsight = getQueryInsight(qid);
    var columns = queryInsight && queryInsight.highlighted_columns ? queryInsight.highlighted_columns : [];
    columns.forEach(function (column, idx) {
      if (!map[column]) {
        map[column] = {
          label: "Helps This Query"
        };
      }
    });
    return map;
  }

  function getRelevantHighlightCards(qid) {
    if (!storageAnalysis || !storageAnalysis.derived || !storageAnalysis.derived.highlight_cards) return [];
    return storageAnalysis.derived.highlight_cards.filter(function (card) {
      return !card.related_queries || !card.related_queries.length || card.related_queries.indexOf(qid) !== -1;
    }).slice(0, 4);
  }

  function getRelevantTechniques(qid) {
    var queryInsight = getQueryInsight(qid);
    var rankedNames = queryInsight && queryInsight.top_techniques ? queryInsight.top_techniques : [];
    if (!storageAnalysis || !storageAnalysis.analysis || !storageAnalysis.analysis.novel_techniques) return [];
    var all = storageAnalysis.analysis.novel_techniques.filter(function (tech) {
      return tech.queries_helped && tech.queries_helped.indexOf(qid) !== -1;
    });
    if (!rankedNames.length) return all.slice(0, 5);

    var byLowerName = {};
    all.forEach(function (tech) {
      byLowerName[String(tech.name || "").toLowerCase()] = tech;
    });

    var picked = rankedNames.map(function (name) {
      var exact = byLowerName[String(name || "").toLowerCase()];
      if (exact) return exact;

      if (queryInsight && queryInsight.top_technique_details) {
        var matchingDetail = queryInsight.top_technique_details.find(function (item) {
          return String(item.display_name || "").toLowerCase() === String(name || "").toLowerCase()
            || String(item.name || "").toLowerCase() === String(name || "").toLowerCase();
        });
        if (matchingDetail && byLowerName[String(matchingDetail.name || "").toLowerCase()]) {
          return byLowerName[String(matchingDetail.name || "").toLowerCase()];
        }
      }

      var viaHighlight = getQueryTechniqueHighlights(qid).find(function (item) {
        return String(item.label || "").toLowerCase() === String(name || "").toLowerCase()
          || String(item.technique_name || "").toLowerCase() === String(name || "").toLowerCase();
      });
      if (viaHighlight && byLowerName[String(viaHighlight.technique_name || "").toLowerCase()]) {
        return byLowerName[String(viaHighlight.technique_name || "").toLowerCase()];
      }

      return all.find(function (tech) {
        var techName = String(tech.name || "").toLowerCase();
        var requested = String(name || "").toLowerCase();
        return techName.indexOf(requested) !== -1 || requested.indexOf(techName) !== -1;
      }) || null;
    }).filter(Boolean);

    if (picked.length) return picked;
    return all.slice(0, 5);
  }

  function getTechniqueLabel(technique) {
    if (!technique) return "Storage idea";
    return technique.label || technique.display_name || technique.name || "Storage idea";
  }

  function getTechniqueBullet(qid, technique) {
    var queryInsight = getQueryInsight(qid);
    if (queryInsight && queryInsight.top_technique_details) {
      var detail = queryInsight.top_technique_details.find(function (item) {
        return item.name === technique.name
          || item.display_name === technique.name
          || item.display_name === getTechniqueLabel(technique);
      });
      if (detail && detail.explanation) return detail.explanation;
    }
    var highlights = getQueryTechniqueHighlights(qid);
    var match = highlights.find(function (item) { return item.technique_name === technique.name; });
    if (match && match.reason) return match.reason;
    return technique.why_its_novel || technique.how_it_works || "Key storage mechanism for this query.";
  }

  function getRelevantCrossTableOptimizations(qid) {
    if (!storageAnalysis || !storageAnalysis.analysis || !storageAnalysis.analysis.cross_table_optimizations) return [];
    var queryInsight = getQueryInsight(qid);
    var tableNames = queryInsight ? (queryInsight.tables_accessed || []) : [];
    return storageAnalysis.analysis.cross_table_optimizations.filter(function (item) {
      return !tableNames.length || intersects(item.tables_involved || [], tableNames);
    }).slice(0, 4);
  }

  function getPrimaryTableBlueprint(qid) {
    if (!storageAnalysis || !storageAnalysis.analysis || !storageAnalysis.analysis.tables) return null;
    var queryInsight = getQueryInsight(qid);
    var tableNames = queryInsight && queryInsight.tables_accessed && queryInsight.tables_accessed.length
      ? queryInsight.tables_accessed
      : inferTablesFromStorage(qid);
    var available = storageAnalysis.analysis.tables.filter(function (table) {
      return tableNames.indexOf(table.table_name) !== -1;
    });
    if (!available.length) return null;
    return available.sort(function (a, b) {
      return (b.column_encodings ? b.column_encodings.length : 0) - (a.column_encodings ? a.column_encodings.length : 0);
    })[0];
  }

  function getRelevantTableInsights(qid) {
    var queryInsight = getQueryInsight(qid);
    if (!queryInsight || !storageAnalysis || !storageAnalysis.derived || !storageAnalysis.derived.table_insights) return [];
    var tableNames = queryInsight.tables_accessed || [];
    return storageAnalysis.derived.table_insights.filter(function (item) {
      return tableNames.indexOf(item.table_name) !== -1;
    }).sort(function (a, b) {
      return b.impact_score - a.impact_score;
    }).slice(0, 4);
  }

  function getRelevantRisks(qid) {
    var queryInsight = getQueryInsight(qid);
    if (!storageAnalysis || !storageAnalysis.derived || !storageAnalysis.derived.design_risks) return [];
    var tableNames = queryInsight ? (queryInsight.tables_accessed || []) : [];
    return storageAnalysis.derived.design_risks.filter(function (risk) {
      return !risk.related_tables || !risk.related_tables.length || intersects(risk.related_tables, tableNames);
    }).slice(0, 3);
  }

  function intersects(a, b) {
    return a.some(function (item) { return b.indexOf(item) !== -1; });
  }

  function countSortKeys(design) {
    if (!design || !design.columns) return 0;
    return design.columns.filter(function (col) { return !!col.sortKey; }).length;
  }

  function countUniqueEncodings(design) {
    if (!design || !design.columns) return 0;
    var seen = {};
    design.columns.forEach(function (col) { seen[col.encoding] = true; });
    return Object.keys(seen).length;
  }

  function buildFallbackQueryNarrative(q) {
    var cols = q.storageDesign && q.storageDesign.columns ? q.storageDesign.columns.length : 0;
    var indexes = q.storageDesign && q.storageDesign.indexes ? q.storageDesign.indexes.length : 0;
    return currentQuery + " uses a tightly scoped physical layout with " + cols + " tuned columns and " + indexes +
      " helper access paths so the generated code can scan less, branch less, and keep hot values closer together.";
  }

  function getOverviewNarrative(q) {
    var queryInsight = getQueryInsight(currentQuery);
    if (queryInsight) return queryInsight.storage_narrative || queryInsight.narrative;
    return buildFallbackQueryNarrative(q);
  }

  function getSelectedEncodingMix(context) {
    if (!context) return [];
    var selectedTable = getSelectedStorageTable(context);
    if (selectedTable && context.table_encoding_mix && context.table_encoding_mix[selectedTable]) {
      return context.table_encoding_mix[selectedTable];
    }
    return context.encoding_mix || [];
  }

  function buildEncodingMixChips(mix) {
    var wrap = document.createElement("div");
    wrap.className = "storage-encoding-chip-row";
    mix.slice(0, 4).forEach(function (entry) {
      var chip = document.createElement("span");
      chip.className = "storage-encoding-chip";
      chip.textContent = humanizeToken(entry.encoding) + " × " + entry.count;
      wrap.appendChild(chip);
    });
    return wrap;
  }

  function getStorageNarrative(q) {
    var queryInsight = getQueryInsight(currentQuery);
    if (queryInsight && queryInsight.storage_narrative) return queryInsight.storage_narrative;
    return buildFallbackQueryNarrative(q);
  }

  function inferTablesFromStorage(qid) {
    var q = data && data.queries ? data.queries[qid] : null;
    if (!q || !q.storageDesign || !q.storageDesign.columns) return [qid];
    var names = [];
    q.storageDesign.columns.forEach(function (col) {
      var prefix = col.name.split("_")[0].toUpperCase();
      var table = {
        C: "CUSTOMER",
        O: "ORDERS",
        L: "LINEITEM",
        P: "PART",
        PS: "PARTSUPP",
        S: "SUPPLIER",
        N: "NATION",
        R: "REGION"
      }[prefix];
      if (table && names.indexOf(table) === -1) names.push(table);
    });
    return names.slice(0, 4);
  }

  function buildFallbackHighlightCards(q) {
    return [
      {
        title: "Query Footprint",
        value: (q.storageDesign && q.storageDesign.columns ? q.storageDesign.columns.length : 0) + " tuned columns",
        detail: "Only the columns that matter to this query are called out, which keeps the storage story concrete and query-shaped."
      },
      {
        title: "Access Paths",
        value: (q.storageDesign && q.storageDesign.indexes ? q.storageDesign.indexes.length : 0) + " specialized indexes",
        detail: "The design pairs encodings with helper structures so filters and joins can skip unnecessary work."
      },
      {
        title: "Pipeline Fit",
        value: q.iterations.length + " code generations",
        detail: "The storage layout gives the optimizer a physical shape it can exploit across base code and later optimization passes."
      }
    ];
  }

  function buildFallbackTechniques(q) {
    var techniques = [];
    if (q.storageDesign && q.storageDesign.indexes) {
      q.storageDesign.indexes.forEach(function (idx) {
        techniques.push({
          name: idx.type + " on " + idx.column,
          table: currentQuery,
          category: "indexing",
          detail: idx.reason
        });
      });
    }
    if (q.storageDesign && q.storageDesign.columns) {
      q.storageDesign.columns.slice(0, 3).forEach(function (col) {
        techniques.push({
          name: col.name + " " + col.encoding,
          table: currentQuery,
          category: "encoding",
          detail: col.reason
        });
      });
    }
    return techniques.slice(0, 5);
  }

  function prettifyInsightValue(title, value) {
    if (!value) return "";
    if (title === "Highest Leverage Table") {
      return String(value).replace(/\s*\(\d+\/100\)/, "");
    }
    if (title === "Best-Supported Query") {
      return String(value).replace(/\s*\(\d+\/100\)/, "");
    }
    if (title === "Design Philosophy") {
      return "Workload-shaped physical design";
    }
    if (title === "Most Novel Technique") {
      return String(value).replace(/\s*\(\d+\/10\)/, "");
    }
    return String(value);
  }

  function friendlyTechniqueBadge(tech) {
    var category = humanizeToken(tech.category || "storage");
    if (tech.queries_helped && tech.queries_helped.length) {
      return tech.queries_helped.length + " query paths";
    }
    if (tech.impact_queries) {
      return tech.impact_queries + " query paths";
    }
    return category;
  }

  function humanizeToken(value) {
    return String(value || "")
      .replace(/_/g, " ")
      .replace(/\b\w/g, function (m) { return m.toUpperCase(); });
  }

  function sanitizeClass(value) {
    return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, "-");
  }

  function shortBottleneckLabel(value) {
    if (!value) return "Mixed";
    var text = String(value);
    if (text.length <= 24) return text;
    return text.split(" ").slice(0, 3).join(" ");
  }

  function qualitativeLeverageLabel(score) {
    if (score >= 90) return "core table";
    if (score >= 70) return "high leverage";
    if (score >= 45) return "important";
    return "supporting";
  }

  function formatRows(n) {
    if (n >= 1000000) return (n / 1000000).toFixed(1) + "M";
    if (n >= 1000) return (n / 1000).toFixed(1) + "K";
    return n.toString();
  }

  // ── SQL Highlighting ──
  function highlightSQL(sql, placeholders) {
    var keywords = /\b(SELECT|FROM|WHERE|AND|OR|NOT|IN|EXISTS|BETWEEN|LIKE|ORDER\s+BY|GROUP\s+BY|HAVING|LIMIT|JOIN|LEFT|RIGHT|INNER|OUTER|ON|AS|CASE|WHEN|THEN|ELSE|END|SUM|AVG|COUNT|MIN|MAX|DISTINCT|INTERVAL|DATE|EXTRACT|YEAR)\b/gi;
    var html = escHtml(sql);
    if (placeholders) {
      Object.keys(placeholders).forEach(function (key) {
        var re = new RegExp("\\[" + key.toUpperCase() + "\\]", "g");
        html = html.replace(re, '<span class="placeholder">[' + escHtml(key.toUpperCase()) + ']</span>');
      });
    }
    html = html.replace(keywords, function (m) { return '<span class="keyword">' + m + '</span>'; });
    html = html.replace(/'([^']*)'/g, '<span class="string">\'$1\'</span>');
    html = html.replace(/\b(\d+\.?\d*)\b/g, function (m, n, offset, str) {
      if (str.substring(0, offset).match(/<[^>]*$/)) return m;
      return '<span class="number">' + n + '</span>';
    });
    return html;
  }

  // ── Performance Chart ──
  function renderPerfChart(q) {
    var container = $("perf-chart");
    container.innerHTML = "";

    var selectedIteration = q.iterations[Math.max(0, Math.min(currentIteration, q.iterations.length - 1))];
    var bestIteration = q.iterations.reduce(function (best, iter) {
      if (!best) return iter;
      return iter.timing_ms < best.timing_ms ? iter : best;
    }, null);
    var humanPersonaIteration = q.iterations.find(function (iter) {
      return String(iter.label || "").toLowerCase() === "human persona";
    }) || null;
    if (bestIteration && humanPersonaIteration && humanPersonaIteration.timing_ms <= bestIteration.timing_ms * 1.001) {
      bestIteration = humanPersonaIteration;
    }
    var bespokeMs = selectedIteration.timing_ms;
    var entries = [{ name: "BespokeOLAP", ms: bespokeMs, cls: "bespoke" }];
    if (q.baselines.DuckDB) entries.push({ name: "DuckDB", ms: q.baselines.DuckDB.timing_ms, cls: "duckdb" });
    if (q.baselines.Umbra) entries.push({ name: "Umbra", ms: q.baselines.Umbra.timing_ms, cls: "umbra" });
    if (q.baselines.ClickHouse) entries.push({ name: "ClickHouse", ms: q.baselines.ClickHouse.timing_ms, cls: "clickhouse" });

    var maxMs = Math.max.apply(null, entries.map(function (e) { return e.ms; }));

    entries.forEach(function (e) {
      var row = document.createElement("div");
      row.className = "perf-bar-row";
      var label = document.createElement("div");
      label.className = "perf-label";
      label.textContent = e.name;
      var track = document.createElement("div");
      track.className = "perf-track";
      var fill = document.createElement("div");
      fill.className = "perf-fill " + e.cls;
      fill.setAttribute("data-target-width", Math.max(3, (e.ms / maxMs) * 100).toFixed(1));
      fill.style.width = "0%";
      var span = document.createElement("span");
      span.textContent = (e.ms / 1000).toFixed(e.ms >= 1000 ? 2 : 3) + "s";
      fill.appendChild(span);
      track.appendChild(fill);
      row.appendChild(label);
      row.appendChild(track);
      container.appendChild(row);
    });

    var duckdbMs = q.baselines.DuckDB ? q.baselines.DuckDB.timing_ms : null;
    var selectedSpeedup = duckdbMs && bespokeMs ? duckdbMs / bespokeMs : null;
    var bestSpeedup = duckdbMs && bestIteration && bestIteration.timing_ms ? duckdbMs / bestIteration.timing_ms : null;
    var isBestSelected = !!bestIteration && selectedIteration === bestIteration;
    var secondary = "";
    if (!isBestSelected && bestIteration && bestSpeedup !== null) {
      secondary =
        '<div class="speedup-secondary">' +
          '<span class="speedup-secondary-label">Best version</span>' +
          '<span class="speedup-secondary-value">' + escHtml(bestIteration.label) + ' · ' + bestSpeedup.toFixed(1) + 'x</span>' +
        '</div>';
    }

    $("speedup-container").innerHTML =
      '<div class="speedup-badge' + (isBestSelected ? '' : ' speedup-badge--muted') + '">' +
        '<span class="speedup-number">' + (selectedSpeedup !== null ? selectedSpeedup.toFixed(1) : '—') + 'x</span>' +
        '<span class="speedup-label">faster than<br>DuckDB</span>' +
      '</div>' +
      secondary;

    requestAnimationFrame(function () {
      requestAnimationFrame(animatePerfBars);
    });
  }

  function animatePerfBars() {
    document.querySelectorAll(".perf-fill[data-target-width]").forEach(function (f) {
      f.style.width = f.getAttribute("data-target-width") + "%";
    });
  }

  // ── Timeline ──
  function renderTimeline(q) {
    var container = $("timeline");
    container.innerHTML = "";
    var iterations = q.iterations;
    var maxMs = Math.max.apply(null, iterations.map(function (it) { return it.timing_ms; }));

    iterations.forEach(function (iter, idx) {
      var item = document.createElement("div");
      item.className = "timeline-item" + (idx === currentIteration ? " active" : "");
      item.addEventListener("click", function () {
        currentIteration = idx;
        container.querySelectorAll(".timeline-item").forEach(function (it, i) {
          it.classList.toggle("active", i === idx);
        });
        renderPerfChart(q);
        // Find and switch to the right pipeline stage for this iteration
        if (q.pipeline) {
          var targetStage = Math.min(idx + 2, q.pipeline.length - 1); // +2 because analysis+storage come first
          currentStage = targetStage;
          $("pipeline-nav").querySelectorAll(".pipeline-btn").forEach(function (b, i) {
            b.classList.toggle("active", i === targetStage);
          });
          renderStageContent(q, targetStage);
        }
      });

      var barHeight = Math.max(8, (iter.timing_ms / maxMs) * 110);
      var bar = document.createElement("div");
      bar.className = "timeline-bar";
      var lightness = 30 + (1 - iter.timing_ms / maxMs) * 25;
      bar.style.background = "hsl(155, 50%, " + lightness + "%)";
      bar.style.height = "0px";
      setTimeout(function () { bar.style.height = barHeight + "px"; }, 80 + idx * 120);

      var timing = document.createElement("div");
      timing.className = "timeline-timing";
      timing.textContent = (iter.timing_ms / 1000).toFixed(iter.timing_ms >= 1000 ? 1 : 2) + "s";

      var label = document.createElement("div");
      label.className = "timeline-label";
      label.textContent = iter.label;
      label.title = iter.label;

      item.appendChild(bar);
      item.appendChild(timing);
      item.appendChild(label);
      container.appendChild(item);
    });
  }

  // ── Heatmap ──
  function renderHeatmap() {
    var container = $("heatmap");
    container.innerHTML = "";
    var queries = data.queries;
    var qids = Object.keys(queries).sort(function (a, b) {
      return parseInt(a.replace(/\D/g, ""), 10) - parseInt(b.replace(/\D/g, ""), 10);
    });

    container.style.gridTemplateColumns = "60px repeat(" + qids.length + ", 1fr)";

    var corner = document.createElement("div");
    corner.className = "heatmap-header";
    container.appendChild(corner);
    qids.forEach(function (qid) {
      var h = document.createElement("div");
      h.className = "heatmap-header";
      h.textContent = qid;
      container.appendChild(h);
    });

    var rowLabel = document.createElement("div");
    rowLabel.className = "heatmap-row-label";
    rowLabel.textContent = "Speedup";
    container.appendChild(rowLabel);

    qids.forEach(function (qid) {
      var q = queries[qid];
      var speedup = q.finalSpeedup;
      var cell = document.createElement("div");
      cell.className = "heatmap-cell" + (qid === currentQuery ? " active" : "");
      cell.textContent = speedup.toFixed(1) + "x";

      var intensity = Math.min(1, (speedup - 1) / 14);
      var r = Math.round(12 + intensity * 15);
      var g = Math.round(22 + intensity * 55);
      var b = Math.round(18 + intensity * 20);
      cell.style.background = "rgb(" + r + "," + g + "," + b + ")";
      cell.style.color = intensity > 0.3 ? "#86efac" : "#5a6a5a";

      cell.addEventListener("click", function () {
        $("sel-query").value = qid;
        window.location.hash = data.benchmark + "/" + qid.toLowerCase();
        window.scrollTo({ top: 0, behavior: "smooth" });
      });
      container.appendChild(cell);
    });
  }

  // ── Scroll Reveal ──
  function setupRevealObserver() {
    if (!("IntersectionObserver" in window)) {
      document.querySelectorAll(".reveal").forEach(function (el) { el.classList.add("visible"); });
      return;
    }
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) entry.target.classList.add("visible");
      });
    }, { threshold: 0.1 });
    document.querySelectorAll(".reveal").forEach(function (el) { observer.observe(el); });
  }

  function triggerReveals() {
    document.querySelectorAll(".reveal").forEach(function (el) { el.classList.remove("visible"); });
    requestAnimationFrame(function () {
      document.querySelectorAll(".reveal").forEach(function (el) {
        if (el.getBoundingClientRect().top < window.innerHeight) el.classList.add("visible");
      });
    });
  }

  // ── Utilities ──
  function escHtml(s) {
    return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  // ── Boot ──
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
