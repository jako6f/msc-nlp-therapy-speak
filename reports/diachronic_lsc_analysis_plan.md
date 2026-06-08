# SIBling-inspired Diachronic Semantic Change Plan (Adapted)

## 0. Overview

* The study treats lexical semantic change as a continuous annual trajectory rather than as a binary epoch-to-epoch contrast. This follows a McTaggart-style time-series orientation: each measure characterises how target terms move through time by target term, year, and comparator group, instead of reducing change to a single pre/post distance score. ([Cambridge Core][4]; [arXiv][5])
* The conceptual frame follows Baes et al.’s SIBling framework because it treats semantic change as something to characterise, not merely detect: instead of collapsing change into one aggregate vector-drift score, it separates interpretable dimensions of Sentiment, Breadth, and Intensity, here supplemented by Salience and Thematic content. This responds to the broader gap noted in semantic-change research, where detection has received more attention than characterisation. ([ACL Anthology][2]; [arXiv][5])
* The design now includes one supervised **Frame Classification** layer before the five operational LSC measures. Frame Classification distinguishes clinical/disorder framing from identity and lived-experience framing in ADHD/autism contexts, so that later semantic trajectories can be interpreted both overall and within substantively meaningful discourse strata.
* The five operational LSC measures remain: **Salience** as relative mention frequency, **Intensity** as affective arousal/severity of target contexts, **Breadth** as contextual dispersion, **Sentiment** as affective valence of target contexts, and **Thematic evolution** as topic structure over time.
* The main empirical comparison is not simply “ADHD/autism changed” but whether ADHD/autism trajectories exceed or differ from broader background drift in comparator terms.
* Across dimensions, the main analysis treats **ADHD** and **Autism** separately. Raw target forms are aggregated into conceptual target groups for the main estimates, while raw-form diagnostics are retained to check whether one form drives a trajectory.
* Comparator terms are included throughout the analysis as separate baseline series (`frustration`, `sadness`, `loneliness`). A composite baseline may be reported only after inspecting the individual baselines for compatible sample sizes, coverage, and trajectories.
* The design deliberately keeps the measures interpretable: count-based prevalence, lexicon-based VAD indices, target-aware contextual dispersion, and topic modelling.
* The first operational change from Baes et al. is that **NRC-VAD v2.1** replaces Warriner et al.’s VAD norms for Intensity and Sentiment because it has wider English coverage, includes common multi-word expressions, and reports human-rated valence, arousal, and dominance scores for more than 55,000 English words and phrases. ([arXiv][1])
* The second operational change is that **XL-LEXEME** replaces a generic sentence embedder for Breadth, because it produces target-aware word-in-context representations rather than general sentence vectors. ([ACL Anthology][3])
* The third operational change is that Thematic evolution will use **bottom-up BERTopic** rather than Baes et al.’s top-down pathologisation dictionary; whether XL-LEXEME embeddings should also feed BERTopic remains a later modelling decision. ([ACL Anthology][2])
* The fourth operational change is that ADHD/autism analyses will be frame-aware. Pisl et al. show that apparent semantic-severity trends can be explained by changing discourse composition rather than intrinsic semantic change alone; in their case, the time effect for depression became nonsignificant after controlling for mental-health context. This project adapts that insight by making clinical/disorder versus lived-experience framing a core stratum rather than a post-hoc robustness check. ([JMIR][6])

### Shared semantic context contract

For Frame Classification, Sentiment, Intensity, Breadth, and Thematic analyses, the primary annual axis is document publication year (`lsc_year = published_year`), not Common Crawl source/capture year. The shared context table therefore keeps only WARC-validated, English, deduplicated contexts with parseable `published_ts` in the 2014-2026 analysis window. `source_year` remains provenance metadata for crawl-composition diagnostics, not the main diachronic variable.

## 1. Frame Classification — Clinical and lived-experience discourse strata

### Concept

ADHD and autism are not used only as diagnostic labels. In web discourse, they can refer to clinical or nosological categories, disorder constructs, support and service categories, identity positions, and everyday lived experiences. Treating all target contexts as one semantic population risks confounding lexical semantic change with a change in discourse composition. Pisl et al. demonstrate this problem for depression: apparent changes in semantic severity were strongly associated with the growth of mental-health contexts, and the independent time trend became nonsignificant once context was controlled. ([JMIR][6])

This project therefore treats substantive target discourse and clinical/disorder versus lived-experience framing as a core supervised classification layer. The aim is not to remove one frame from the analysis, but to separate four questions: how much target material is substantive rather than boilerplate or incidental; how the substantive frame composition changes over time; how each frame changes internally; and how much the overall trajectory depends on changing frame proportions.

### Operational definition

Each ADHD/autism mention passage is labelled hierarchically. Stage 0 first determines whether there is enough coherent, target-specific ADHD/autism discourse to classify the frame:

* `substantive_target_discourse`: the passage contains enough coherent, target-specific discourse about ADHD/autism, ADHD/autistic people, traits, diagnosis, support, treatment, services, research, stigma, inclusion, or everyday life to classify framing. Thin, navigational, list-like, promotional, generic, incidental, noisy, or garbled passages are coded `FALSE`; uncertain cases are coded `FALSE`.

Stage 1 is labelled only when `substantive_target_discourse = TRUE`:

* `clinical_frame_present`: the passage frames ADHD/autism as a diagnosis, disorder, condition, symptom profile, impairment, treatment target, service category, research or epidemiological category, DSM/ICD-style construct, medication issue, or clinical/educational support need.
* `lived_experience_frame_present`: the passage frames ADHD/autism as identity, self-understanding, first-person or family experience, neurodivergent community, masking, stigma, accommodation, everyday coping, belonging, pride, or embodied/social experience.

If `substantive_target_discourse = FALSE`, clinical and lived-experience labels are not applicable and are stored as `NA`, not as ordinary negative frame examples. The reported strata are derived deterministically:

| substantive target discourse | clinical frame | lived-experience frame | derived frame |
|---|---|---|---|
| no | NA | NA | `non_substantive_or_insufficient` |
| yes | yes | no | `clinical_only` |
| yes | no | yes | `lived_only` |
| yes | yes | yes | `mixed` |
| yes | no | no | `substantive_other` |

The annotation unit is `target_sentence_plus_adjacent` from the shared LSC context table. Baseline terms are not frame-labelled because the clinical/lived distinction is substantively motivated for ADHD/autism rather than for the comparator emotion terms.

### Annotation and classifier workflow

The gold-standard workflow is human-led but LLM-assisted. It adapts the annotator-critic-human-correction structure proposed in ACT: an LLM annotator labels examples, a criticizer model estimates likely annotation errors, and a human corrects suspicious labels before the data are used downstream. ([arXiv][7]) The ACT paper treats critic thresholds as budget- and task-dependent rather than prescribing a universal numeric cut-off, so this project will calibrate review priority empirically during the pilot rather than adopting a fixed value.

The planned workflow is:

1. Write and pilot a hierarchical codebook with a Stage-0 substantive-discourse gate and conditional clinical/lived-experience frame labels.
2. Create a 200-case human pilot set and a separate 400-case human validation set, stratified by target group and broad year band.
3. Refine compact, schema-constrained LLM prompts qualitatively on pilot cases, following general prompt-engineering guidance to define the task, specify output format, and use representative examples where they materially reduce errors. ([OpenAI Docs][8])
4. Use an OpenAI/Codex interface for the LLM annotator and Claude Code for cross-model criticism, storing prompts, model/interface metadata, raw outputs, parsed outputs, and correction sheets. ([Anthropic Docs][9])
5. Human-correct critic-flagged LLM labels where feasible; if the critic flags an unmanageably high share, revise the codebook/prompt/model pairing rather than treating correction volume as merely a workload problem.
6. Train a hierarchical classifier from shared sentence-transformer embeddings: one head predicts `p_substantive` on all labelled examples, while clinical and lived-experience heads predict `p_clinical_given_substantive` and `p_lived_given_substantive` only from substantive examples. This keeps the model simple and inspectable while avoiding the error of treating non-substantive boilerplate as meaningful negative evidence for clinical or lived framing. ([ACL Anthology][10])
7. Evaluate only on the held-out human validation set before applying the classifier to all ADHD/autism contexts.

### Outputs

* Protected annotation-ready pilot and validation XLSX workbooks with context fields stored as text and constrained label dropdowns.
* Codebook and locked annotator/critic prompts.
* LLM annotation batches, critic batches, and correction handoffs.
* Classifier validation metrics for the substantive, clinical-given-substantive, lived-given-substantive, and derived-frame outputs.
* Frame labels, probabilities, and optional probability-derived frame weights for all ADHD/autism target contexts.
* Annual frame-composition summaries by target group.

### Use in downstream LSC analyses

For ADHD and autism, downstream analyses should report both overall trajectories and frame-aware trajectories where sample size permits. Frame-stratified estimates should use substantive contexts and answer whether clinical, lived-experience, mixed, or substantive-other contexts show different semantic patterns. The non-substantive category is primarily a corpus-quality and discourse-composition diagnostic rather than a semantic frame. A secondary Pisl-inspired sensitivity analysis can compare raw annual scores with frame-adjusted models or standardised frame-composition estimates. This should be described as an adaptation of Pisl et al.'s context-control logic, not as a direct replication of their exact model.

### Key assumptions + likely failure modes

* **Assumption:** substantive target discourse and clinical/lived-experience frames are recoverable from short target-centred passages.
* **Assumption:** conditional clinical and lived-experience axes are more faithful than a forced single-label schema because clinical and lived-experience framing can co-occur.
* **Failure mode:** LLM labels may be systematically biased toward surface cues or overconfident in noisy web passages.
* **Failure mode:** one human annotator limits traditional inter-annotator reliability claims; human-LLM agreement should be reported as a diagnostic, not as independent human reliability.
* **Failure mode:** the Stage-0 gate may exclude borderline generic support/resource language that contains weak but potentially relevant discourse signal.
* **Failure mode:** frame-year cells may be too small for stable stratified LSC estimates, especially for `mixed` and `substantive_other` strata.
* **Failure mode:** a classifier trained on LLM-assisted labels may reproduce LLM errors unless the human validation set is kept strictly separate and reported transparently.

## 2. Measure 1 — Salience (Prevalence)

### Concept (Baes et al. framing)

Salience captures how prominent a lexical item or concept becomes in a corpus over time. In Baes et al., salience is not one of the three primary semantic dimensions, but it is treated as a complementary indicator that helps interpret whether semantic shifts occur alongside increasing cultural or disciplinary attention. ([ACL Anthology][2])

For this project, Salience is the frequency with which ADHD/autism-related expressions appear in general web discourse over annual Common Crawl slices. It answers a different question from semantic change: not “what does the term mean?”, but “how often is the term invoked?” For ADHD/autism, frame composition is also a salience-like outcome: it estimates how much target discourse is non-substantive, clinical-only, lived-only, mixed, or substantive-other in each year.

Unlike Frame Classification, Sentiment, Intensity, Breadth, and Thematic analyses, Salience uses Common Crawl source year as its primary annual axis. This is a deliberate denominator choice: the available denominator is the fixed yearly WET sample scanned for target and baseline terms. Publication-year diagnostics are retained to quantify leakage, but publication year is not used as the main Salience denominator because documents without term hits do not receive WARC extraction or publication-date recovery.

### Operational definition (my pipeline)

For each analysis unit $u$ and year $Y$, compute annual relative frequency using the same token denominator for target groups and comparator terms:

$$
\begin{aligned}
\text{Salience}^{\text{WET}}_{u,Y}
&=
\frac{H^{\text{WET}}_{u,Y}}{T_Y}, \\
\text{Salience}^{\text{WARC}}_{u,Y}
&=
\frac{H^{\text{WARC}}_{u,Y}}{T_Y}, \\
\text{Retention}_{u,Y}
&=
\frac{H^{\text{WARC}}_{u,Y}}{H^{\text{WET}}_{u,Y}}.
\end{aligned}
$$

where $Y$ is Common Crawl source year, $H^{\text{WET}}_{u,Y}$ is the number of WET-validated hits for analysis unit $u$, $H^{\text{WARC}}_{u,Y}$ is the number of WARC-validated hits, and $T_Y$ is the annual WET token denominator for minimum-length documents entering term matching. Retention is defined only when $H^{\text{WET}}_{u,Y} > 0$. The reported rate should normally be rescaled to hits per million WET tokens.

This aligns the salience measure more closely with Baes et al.’s normalised target frequency while preserving the project’s WET-first, WARC-second validation design. WARC salience is the cleaner substantive trend signal; WET salience and candidate-hit rates remain important diagnostics for checking whether validation changes the temporal pattern. Document-denominated rates are retained as supplementary diagnostics because the collection pipeline naturally records scanned-document counts.

Analysis units:

* ADHD: `adhd`, `attention[-\s]?deficit`
* Autism: `autism`, `autistic`, `autism[-\s]?spectrum`, and disambiguated `ASD`
* Baselines: `frustration`, `sadness`, `loneliness`

### High-level implementation plan (step-by-step; no code)

#### Inputs needed

* Annual Common Crawl WET scan outputs.
* WARC validation outputs.
* Per-year denominators: tokens and documents entering the WET scan/matching stage.
* Term-level hit tables with year, crawl ID, URL/domain, target group, raw term, validation status.
* Comparator-term results processed through the same pipeline.

#### Preprocessing assumptions

* One deterministic crawl slice per year.
* Same scan logic, document-length threshold, target regexes, ASD disambiguation rule, and domain cap across all years.
* WET candidates and comparator candidates pass through identical filtering and validation logic.
* WARC validation is treated as the cleaner signal, but WET counts remain important because they preserve a transparent denominator.

#### Computation steps

1. Aggregate scanned-token and scanned-document counts by year.
2. Aggregate WET candidate hits and WET-validated hits by year, analysis unit, and raw term.
3. Aggregate WARC-validated hits by year, analysis unit, and raw term.
4. Compute token-denominated relative frequencies using the same denominator for all target and comparator groups.
5. Compute WARC/WET retention rates to assess how much validation changes the temporal pattern.
6. Produce annual time series for ADHD, autism, and comparator terms.
7. Where useful, normalise each term’s trajectory to its own baseline period to compare temporal shape rather than absolute frequency.

#### Outputs (tables/figures)

* Annual table: year, analysis unit, tokens scanned, docs scanned, WET hits, WARC hits, token-denominated rates, document-denominated diagnostic rates, WARC/WET retention.
* Line plot: WET and WARC relative frequencies over time.
* Line plot: ADHD/autism versus comparator trajectories.
* Annual frame-composition table and plot for ADHD/autism target contexts.
* Domain concentration table: top domains per year and target group.
* Retention plot: WARC-validated hits as a proportion of WET-validated hits.

#### Diagnostics / sanity checks

* Check whether large salience spikes are driven by one domain, duplicated pages, crawler artefacts, or page-template contamination.
* Compare WET and WARC trends. If WET rises but WARC does not, the apparent trend may be boilerplate or page-structure artefact.
* Inspect annual WARC/WET retention rates. Large year-specific drops may indicate extraction, crawl-composition, or URL-resolution issues.
* Inspect publication-year diagnostics to check how much WARC-validated material falls outside the intended 2014-2026 publication window.
* Check whether comparator terms show similar temporal drift, because general web language and Common Crawl composition will change over time.
* Track hit-per-document and document-per-domain distributions to detect concentration.

#### Efficiency notes for ~1,000–1,500 docs per term per year

Salience is cheap once the scan and validation outputs exist. Aggregation is linear in the number of rows. The main efficiency concern is not computation but reproducibility: denominators, crawl IDs, validation statuses, and filtering parameters must be frozen and documented.

### Key assumptions + likely failure modes

* **Assumption:** the annual Common Crawl slices are comparable enough to support relative-frequency trends.
* **Assumption:** WARC validation removes enough boilerplate to make the salience signal substantively interpretable.
* **Failure mode:** crawl composition changes masquerade as discourse change.
* **Failure mode:** high-volume domains dominate a target-year.
* **Failure mode:** ADHD/autism terms appear in navigation, accessibility widgets, product pages, or directory pages rather than substantive prose.
* **Failure mode:** salience is overinterpreted as public concern or prevalence. It should be interpreted only as web-discourse prominence within the sampled corpus.

## 3. Measure 2 — Intensity (Vertical creep)

### Concept (Baes et al. framing)

Intensity corresponds to whether a term’s usage shifts toward more or less emotionally intense contexts. In concept-creep terms, vertical creep refers to harm- or pathology-related concepts extending to less severe phenomena. Baes et al. operationalise intensity partly through affective arousal of collocates and partly through intensifying modifiers. ([ACL Anthology][2])

For this project, Intensity is an annual measure of how emotionally activated or severity-laden the local contexts of ADHD/autism terms are. A falling arousal/severity trajectory may be consistent with vertical creep, but it is not sufficient evidence on its own. It must be interpreted alongside Breadth, Sentiment, thematic context, and the clinical/lived-experience frame composition of target contexts.

### Operational definition (my pipeline)

For each analysis unit and year:

* extract ±5-word collocate windows around target mentions;
* match collocates to **NRC-VAD v2.1**;
* compute an annual weighted mean **arousal** score;
* where feasible, compute a secondary severity-oriented modifier/collocate subindex using a small transparent severity lexicon.

Formally, for analysis unit $u$ in year $Y$:

$$
\text{Intensity}_{u,Y}
=
\frac{
\sum_{w \in C_{u,Y}} f_{w,u,Y} \cdot A(w)
}{
\sum_{w \in C_{u,Y}} f_{w,u,Y}
}
$$

where $C_{u,Y}$ is the set of NRC-VAD-matched collocates appearing in the target windows for analysis unit $u$ in year $Y$, $f_{w,u,Y}$ is the frequency of collocate $w$, and $A(w)$ is the NRC-VAD arousal score for $w$.

This departs from Baes et al.’s use of Warriner et al. because NRC-VAD v2.1 has human-rated valence, arousal, and dominance scores for more than 55,000 English words and phrases, including common multi-word expressions; scores are continuous and suitable for weighted annual aggregation. ([arXiv][1])

#### Supplementary Baes-style severity/intensifier check

Baes et al. also compute an intensity-oriented modifier index from dependency-parsed adjective modifiers of the target term. This is worth implementing here as a supplementary validity check rather than as the primary Intensity measure. The practical version is to dependency-parse only target-containing sentences or short target-centred passages, then count fixed severity/intensifier modifiers that directly modify the target or its head construction.

For analysis unit $u$ in year $Y$:

$$
\text{SeverityModifier}_{u,Y}
=
\frac{M_{u,Y}}{H^{\text{WARC}}_{u,Y}}
$$

where $M_{u,Y}$ is the count of WARC-validated target mentions with a fixed severity/intensifier modifier and $H^{\text{WARC}}_{u,Y}$ is the number of WARC-validated target mentions. This check is useful if it converges with the NRC-VAD arousal trajectory; disagreement should be interpreted substantively rather than treated as a failure.

### High-level implementation plan (step-by-step; no code)

#### Inputs needed

* WARC-extracted, quality-gated documents.
* Target mention table with document ID, year, target group, raw target form, sentence/context location.
* Tokenised and optionally lemmatised text.
* NRC-VAD v2.1 lexicon.
* Small severity/intensifier lexicon, fixed before analysis, for the supplementary modifier check.

#### Preprocessing assumptions

* Use only WARC-validated, English, deduplicated documents for semantic measures.
* Use target-centred local windows, not full documents.
* Exclude the target term itself from collocate scoring.
* Tokenisation, lowercasing, lemmatisation, stopword removal, and punctuation removal should be consistent across all years and all target/comparator groups.
* Multi-word VAD matching should be attempted before unigram matching if practically feasible, because NRC-VAD v2 includes phrase entries.

#### Computation steps

1. For every validated target mention, extract a ±5-token window.

2. Remove the target span itself.

3. Tokenise and lemmatise collocates, applying the same preprocessing for every year.

4. Match collocates or phrases to NRC-VAD arousal scores.

5. Count matched collocates by year, analysis unit, and term.

6. Compute the annual weighted mean arousal score using the formula above.

7. Record coverage:

   * total collocates;
   * matched collocates;
   * coverage rate;
   * number of unique matched collocates.

8. Compute the supplementary Baes-style severity/intensifier modifier index where dependency parses are reliable.

9. Model the annual trajectory with simple trend summaries and plots, not just first-versus-last comparisons.

#### Outputs (tables/figures)

* Annual arousal index by target group and comparator.
* Coverage table by year and target group.
* Top arousal-contributing collocates per decade or year group.
* Line plot of annual arousal trajectories.
* Supplementary severity/intensifier modifier plot.
* Bootstrap confidence intervals for annual scores if sample sizes permit.

#### Diagnostics / sanity checks

* Inspect years with unusually low VAD coverage.
* Check whether high-arousal scores are driven by a small number of repeated collocates.
* Compare ADHD/autism trajectories to comparator trajectories.
* Manually inspect high- and low-arousal windows for face validity.
* Check whether arousal changes are genre-driven, e.g. news/crime pages versus support/information pages.
* Separate ADHD and autism before aggregating, because they may have different intensity trajectories.

#### Efficiency notes for ~1,000–1,500 docs per term per year

The collocate-based VAD calculation is computationally light. Dependency parsing is more expensive but feasible if restricted to target-containing sentences or short passages. The modifier index should remain supplementary because it is higher precision but narrower than the arousal index.

### Key assumptions + likely failure modes

* **Assumption:** local collocates reflect the affective/severity framing of the target usage.
* **Assumption:** NRC-VAD scores are sufficiently stable for web-discourse analysis, despite being a synchronic lexicon.
* **Failure mode:** arousal is not identical to clinical severity. High arousal can reflect moral concern, stigma, advocacy, newsworthiness, or crisis framing.
* **Failure mode:** low arousal does not automatically mean vertical creep; it may reflect informational, bureaucratic, or support-oriented language.
* **Failure mode:** lexicon coverage varies across years, especially with slang, misspellings, identity terms, or multi-word expressions.
* **Failure mode:** target mentions in lists or medical pages can distort collocate distributions even after WARC filtering.

## 4. Measure 3 — Breadth (Horizontal creep)

### Concept (Baes et al. framing)

Breadth corresponds to the diversity of contexts in which a target term appears. In concept-creep terms, horizontal creep means that a concept expands to cover a broader range of phenomena. Baes et al. operationalise breadth as average inverse cosine similarity among sentence embeddings containing the target term, while noting that this captures quantitative contextual dispersion rather than directly identifying qualitatively distinct senses. ([ACL Anthology][2])

For this project, Breadth is the central distributional semantic measure. It asks whether ADHD/autism terms are used in increasingly diverse contexts across annual slices, both overall and within clinical/lived-experience frames where sample size permits.

### Operational definition (my pipeline)

For each analysis unit and year:

* use all usable target-containing contexts up to a fixed annual cap;
* mark the target span;
* encode the marked contexts using **XL-LEXEME**;
* compute contextual dispersion as average pairwise cosine distance within each unit-year sample.

Formally, for analysis unit $u$ in year $Y$, with $N_{u,Y}$ sampled target-containing contexts and XL-LEXEME vectors $\mathbf{v}_1, \ldots, \mathbf{v}_{N_{u,Y}}$:

$$
\text{Breadth}_{u,Y}
=
\frac{2}{N_{u,Y}(N_{u,Y}-1)}
\sum_{i=1}^{N_{u,Y}-1}
\sum_{j=i+1}^{N_{u,Y}}
\left(
1 -
\frac{\mathbf{v}_i \cdot \mathbf{v}_j}
{\lVert \mathbf{v}_i \rVert \lVert \mathbf{v}_j \rVert}
\right)
$$

Higher values indicate greater average contextual dissimilarity among target uses in that year.

As an auxiliary Breadth diagnostic, adjacent-year drift can also be computed using Cassotti et al.’s cross-period average-distance logic: ([ACL Anthology][3])

$$
\text{Drift}_{u,Y \rightarrow Y+1}
=
\frac{1}{N_{u,Y}N_{u,Y+1}}
\sum_{i=1}^{N_{u,Y}}
\sum_{j=1}^{N_{u,Y+1}}
\delta
\left(
\mathbf{v}^{Y}_i,
\mathbf{v}^{Y+1}_j
\right)
$$

with

$$
\delta(\mathbf{a}, \mathbf{b})
=
1 -
\frac{\mathbf{a} \cdot \mathbf{b}}
{\lVert \mathbf{a} \rVert \lVert \mathbf{b} \rVert}
$$

This auxiliary statistic should not replace the within-year Breadth measure; it is useful for checking whether annual usage distributions move between adjacent years.

This replaces Baes et al.’s generic sentence-embedding approach with a target-aware word-in-context model. XL-LEXEME is appropriate here because it was designed for lexical semantic change detection by producing comparable target-marked contextual representations and computing semantic change through average distances between contextual representations. ([ACL Anthology][2]; [ACL Anthology][3])

### High-level implementation plan (step-by-step; no code)

#### Inputs needed

* WARC-extracted, quality-gated documents.
* Target-containing sentences with year, target group, raw target form, document ID, and domain.
* Target-span offsets or reliable target-span matching.
* Pretrained XL-LEXEME model and tokenizer.
* Fixed sampling parameters per target group, baseline term, and year.

#### Preprocessing assumptions

* Use target-containing sentences as the primary unit, with target sentence plus adjacent sentence(s) as a fallback when the sentence is too short or underspecified.
* Explicitly mark the target span using XL-LEXEME’s expected target delimiters.
* Keep raw sentence context sufficiently intact; excessive lemmatisation or stopword removal is inappropriate for contextual embeddings.
* Treat ADHD and autism separately before any aggregate reporting.
* Keep target-form metadata, because `autism`, `autistic`, `ASD`, and `autism spectrum` may behave differently.
* Use the same maximum context length, truncation rule, random seed, and batch settings for all years.

#### Computation steps

1. Extract all valid target-containing sentences for each analysis unit and year.
2. Remove duplicate or near-duplicate sentences.
3. If more than the annual cap is available, use a domain-stratified random sample; a provisional cap of 2,000 contexts per target group-year or baseline-term-year is reasonable subject to a CPU benchmark.
4. Mark the target span in each sentence.
5. Encode each marked sentence with XL-LEXEME.
6. Compute annual contextual dispersion from L2-normalised embeddings, using the closed-form mean-pairwise cosine distance rather than materialising all pairwise distances except for diagnostics.
7. Average distances to obtain the annual Breadth score.
8. Repeat sampling several times or bootstrap contexts to obtain uncertainty intervals.
9. Produce annual trajectories for ADHD, autism, and comparator terms.
10. Optionally compute adjacent-year or anchor-period distances as auxiliary diagnostics, but keep the primary Breadth measure as within-year contextual dispersion.

#### Outputs (tables/figures)

* Annual Breadth score by target group and comparator.
* Bootstrap confidence intervals.
* Sample-size table showing available and sampled contexts per year.
* Line plot of annual contextual dispersion.
* Optional comparison of raw target forms within ADHD/autism groups.
* Optional nearest/farthest context examples for interpretability.

#### Diagnostics / sanity checks

* Check whether annual Breadth increases are driven by domain diversification rather than semantic broadening.
* Inspect far-apart sentence pairs to assess whether XL-LEXEME is capturing meaningful target-use differences.
* Inspect nearest-neighbour clusters to see whether stable usage types are coherent.
* Compare ADHD/autism Breadth trajectories against comparator terms to estimate excess drift.
* Use a chronologically shuffled null if feasible: shuffle year labels, recompute Breadth, and confirm that the observed trajectory is not purely a sampling artefact.
* If feasible, run a standard sentence embedder as a robustness check. This is optional and should be used only to distinguish target-aware semantic dispersion from generic topical sentence diversity.

#### Efficiency notes for ~1,000–1,500 docs per term per year

XL-LEXEME inference is more expensive than a lightweight sentence-transformer model, but still feasible if context counts are capped. Pairwise distance computation is the main scaling issue:

* 200 contexts/year produces 19,900 pairwise distances;
* 500 contexts/year produces 124,750 pairwise distances;
* 1,500 contexts/year produces over 1.1 million pairwise distances.

A practical MSc configuration is therefore to use all available contexts up to a fixed annual cap, store embeddings for reuse, and compute uncertainty from resampling over documents where possible. GPU inference is preferable, but batched CPU inference may be acceptable for capped samples after a small benchmark.

### Key assumptions + likely failure modes

* **Assumption:** XL-LEXEME embeddings capture target-specific contextual meaning better than generic sentence embeddings.
* **Assumption:** annual contextual dispersion is a valid proxy for horizontal semantic broadening.
* **Failure mode:** increasing dispersion may reflect changing web genres or domains rather than concept expansion.
* **Failure mode:** XL-LEXEME was benchmarked primarily on period-comparison lexical semantic change tasks, not on annual McTaggart-style trajectories.
* **Failure mode:** phrase targets and ambiguous acronyms may be represented less cleanly than simple single-token targets.
* **Failure mode:** if yearly sample sizes are too small or uneven, Breadth estimates will be unstable.
* **Failure mode:** contextual dispersion does not identify which new senses or uses emerged; it must be interpreted alongside thematic analysis and manual examples.

## 5. Measure 4 — Sentiment (Connotation)

### Concept (Baes et al. framing)

Sentiment captures whether a term’s connotational environment becomes more positive or more negative over time. In historical semantics this corresponds to amelioration or pejoration. Baes et al. operationalise this through the valence of collocates around target terms. ([ACL Anthology][2])

For this project, Sentiment measures whether ADHD/autism-related discourse becomes more positive, negative, or neutral in local linguistic context. This is not the same as stigma, but it is a useful connotational proxy. For ADHD/autism, overall valence trends should be interpreted alongside frame-stratified trends because clinical contexts may carry systematically different affective language from lived-experience or identity contexts.

### Operational definition (my pipeline)

For each analysis unit and year:

* extract ±5-word collocate windows around target mentions;
* match collocates to **NRC-VAD v2.1**;
* compute an annual weighted mean **valence** score.

Formally, for analysis unit $u$ in year $Y$:

$$
\text{Sentiment}_{u,Y}
=
\frac{
\sum_{w \in C_{u,Y}} f_{w,u,Y} \cdot V(w)
}{
\sum_{w \in C_{u,Y}} f_{w,u,Y}
}
$$

where $C_{u,Y}$ is the set of NRC-VAD-matched collocates appearing in the target windows for analysis unit $u$ in year $Y$, $f_{w,u,Y}$ is the frequency of collocate $w$, and $V(w)$ is the NRC-VAD valence score for $w$.

This mirrors the Intensity procedure but uses NRC-VAD valence rather than arousal. NRC-VAD v2.1 provides continuous VAD scores and is available through Mohammad’s NRC-VAD project page. ([arXiv][1])

### High-level implementation plan (step-by-step; no code)

#### Inputs needed

* Same WARC-validated target-window data used for Intensity.
* NRC-VAD v2.1 valence scores.
* Annual term and comparator metadata.

#### Preprocessing assumptions

* Use the same tokenisation, lemmatisation, phrase matching, and stopword policy as for Intensity.
* Exclude the target term itself from scoring.
* Keep coverage diagnostics parallel to Intensity so valence and arousal are comparable.
* Avoid changing preprocessing rules after inspecting sentiment trends.

#### Computation steps

1. Extract ±5-token windows around each target mention.
2. Remove target spans and non-lexical tokens.
3. Match collocates or phrases to NRC-VAD valence scores.
4. Count matched collocates by analysis unit and year.
5. Compute the annual weighted mean valence score using the formula above.
6. Record annual coverage and unique matched-collocate counts.
7. Compare ADHD/autism trajectories with comparator terms.
8. Inspect the collocates contributing most to high-valence and low-valence years.
9. Plot annual trajectories with uncertainty intervals where feasible.

#### Outputs (tables/figures)

* Annual valence index by target group and comparator.
* Coverage table by year.
* Top positive and negative contributing collocates by period.
* Line plot of Sentiment trajectories.
* Optional combined VAD figure showing valence and arousal side by side.

#### Diagnostics / sanity checks

* Check whether sentiment changes are driven by a few frequent collocates such as `support`, `disorder`, `struggle`, `diagnosis`, `identity`, or `treatment`.
* Inspect whether comparator terms show similar valence drift.
* Separate clinical, support, advocacy, and identity contexts in manual examples where possible.
* Check whether valence drops reflect pathologising language, crisis framing, stigma, or simply more clinical terminology.
* Record VAD coverage every year; do not interpret valence shifts if coverage is sparse or unstable.

#### Efficiency notes for ~1,000–1,500 docs per term per year

Sentiment is computationally cheap and can share almost all preprocessing outputs with Intensity. The main efficiency gain is to build one reusable target-window table and run both valence and arousal aggregation from it.

### Key assumptions + likely failure modes

* **Assumption:** local collocate valence approximates the connotational framing of the target.
* **Assumption:** NRC-VAD’s broader coverage improves robustness relative to smaller VAD resources.
* **Failure mode:** valence of surrounding words is not the same as attitude toward the target.
* **Failure mode:** clinical contexts may appear negative because they discuss impairment or treatment, not because they stigmatise the group.
* **Failure mode:** identity-affirming and support-oriented discourse may contain negative terms because it discusses discrimination or barriers.
* **Failure mode:** lexicon-based sentiment cannot resolve sarcasm, negation, or target-specific evaluative stance.

## 6. Measure 5 — Thematic evolution

### Concept (Baes et al. framing)

Baes et al. treat thematic content as a complementary interpretive layer: it identifies what kinds of contexts or themes surround the target term. Their implementation uses a top-down pathologisation dictionary. ([ACL Anthology][2])

For this project, the thematic layer should be bottom-up because the relevant ADHD/autism frames are not limited to pathologisation. Likely themes may include diagnosis, schooling, workplace accommodation, identity, parenting, neurodiversity, treatment, online self-description, and support communities. These should emerge from the corpus rather than being imposed only through a predefined dictionary. Frame labels should be used as metadata for interpretation and, where sample size permits, for frame-specific topic summaries.

### Operational definition (my pipeline)

Use BERTopic on the Corpus Track to estimate annual topic structure from target-centred passages:

1. embeddings;
2. UMAP dimensionality reduction;
3. HDBSCAN clustering;
4. c-TF-IDF topic representation;
5. `topics_over_time` using annual bins.

Unlike the other four measures, Thematic evolution should not be reduced to one scalar index in the main design. Its primary outputs are topic labels, representative documents, and annual topic-prevalence trajectories. A compact topic-share statistic can be computed internally, but the interpretation should remain qualitative-quantitative rather than a single “theme score”.

The embedding choice remains an explicit modelling decision. Standard document/sentence embeddings may produce more interpretable topics because BERTopic is designed around document-level semantic similarity. XL-LEXEME embeddings may be attractive if the goal is to cluster target-specific usages rather than whole-document themes. The decision should be based on empirical diagnostics: topic coherence, manual interpretability, outlier rate, temporal stability, and whether topics are target-relevant rather than generic web-page themes.

### High-level implementation plan (step-by-step; no code)

#### Inputs needed

* WARC-extracted, English, deduplicated Corpus Track documents.
* Year metadata for each document.
* Target group and raw target-form metadata.
* Clean text field: target-centred passage, with full document or paragraph variants retained only as robustness checks if useful.
* BERTopic stack: embedding model, UMAP, HDBSCAN, c-TF-IDF.
* Stopword list and optional domain-specific term handling.

#### Preprocessing assumptions

* Topic modelling should use substantive text, not WET snippets.

* The provisional modelling unit is a target-centred passage, such as the target-containing sentence plus adjacent sentence context. Full documents capture broader discourse themes but risk diluting target-specific usage, while sentence-only windows may be too short for stable topic modelling.

* ADHD and autism should initially be modelled separately or at least compared separately after modelling.

* Annual topic trends require stable year metadata and enough documents per year.

* Very rare years or target groups may need pooling or cautious interpretation.

#### Computation steps

1. Build target-centred passages and keep enough metadata to inspect whether topics differ by target group, raw form, year, and domain.
2. Clean text minimally: remove obvious boilerplate residue, excessive whitespace, and near-duplicates, while preserving lexical content.
3. Generate embeddings using the selected embedding model.
4. Reduce dimensionality with UMAP.
5. Cluster with HDBSCAN.
6. Generate topic representations with c-TF-IDF.
7. Manually inspect topic labels using top terms and representative documents.
8. Use `topics_over_time` with annual bins.
9. Plot topic prevalence trajectories by year.
10. Compare ADHD/autism topic trajectories with comparator-term topic trajectories only if the topic spaces are substantively comparable after inspection.

#### Outputs (tables/figures)

* Topic inventory: topic ID, label, top terms, representative documents.
* Annual topic-prevalence table.
* Topic-over-time plots for major topics.
* Outlier/noise proportion by target group and year.
* Representative excerpts for key topics.
* Optional alluvial or stream plot showing topic shares over time.

#### Diagnostics / sanity checks

* Topic coherence: top words and representative documents should describe the same theme.
* Target relevance: topics should be about ADHD/autism discourse, not generic website types.
* Temporal stability: topic labels should not change meaning across years.
* Outlier rate: excessive HDBSCAN noise suggests poor embedding choice, overly short documents, or parameter mismatch.
* Domain concentration: a topic should not be entirely one domain unless that is substantively meaningful and reported.
* Robustness: compare results from one alternative embedding or parameter setting only if time permits.

#### Efficiency notes for ~1,000–1,500 docs per term per year

BERTopic is feasible at this scale if run separately for major target groups and with saved embeddings. The main computational cost is embedding and UMAP/HDBSCAN fitting. The main research cost is interpretation: topic models require manual validation and careful labelling. The MVP should avoid excessive parameter sweeps.

### Key assumptions + likely failure modes

* **Assumption:** bottom-up topics recover substantively meaningful discourse frames.
* **Assumption:** annual topic prevalence can be interpreted as thematic evolution if the corpus construction is stable.
* **Failure mode:** topics reflect website genre, domain, or page type rather than discourse frame.
* **Failure mode:** BERTopic topics can be unstable across parameter choices.
* **Failure mode:** topics may conflate clinical, identity, support, and advocacy language if the modelling unit is too broad.
* **Failure mode:** XL-LEXEME embeddings may be too target-specific or context-window-specific for ordinary BERTopic topic coherence; standard sentence/document embeddings may be more suitable, but this should be determined by diagnostics rather than assumed.
* **Failure mode:** `topics_over_time` smooths or aggregates topic terms in ways that can imply stability where actual topic composition shifts.

### Optional enhancements

* **Optional:** run a standard sentence embedder for Breadth as a robustness check against XL-LEXEME.
* **Optional:** add a chronologically shuffled null for Breadth.
* **Optional:** compare the dependency-based severity/intensifier index with a simpler window-based severity lexicon if parsing quality is uneven.
* **Optional:** compare BERTopic with XL-LEXEME embeddings versus a standard document/sentence embedder if time and compute allow.
* **Optional:** stratify trajectories by domain class or broad page genre if reliable labels become available.
* **Optional:** use bootstrapped confidence intervals across all semantic indices, not only Breadth.
* **Optional:** compare frame-adjusted LSC trajectories using hard frame labels versus classifier frame probabilities.


[1]: https://arxiv.org/abs/2503.23547?utm_source=chatgpt.com "NRC VAD Lexicon v2: Norms for Valence, Arousal, and Dominance for over 55k English Terms"
[2]: https://aclanthology.org/2024.acl-long.76/ "A Multidimensional Framework for Evaluating Lexical Semantic Change with Social Science Applications"
[3]: https://aclanthology.org/2023.acl-short.135/ "XL-LEXEME: WiC Pretrained Model for Cross-Lingual LEXical sEMantic changE"
[4]: https://www.cambridge.org/core/journals/natural-language-engineering/article/stateoftheart-of-semantic-change-computation/CCD69C7C2306B0E4D246B456E236EFAF "A state-of-the-art of semantic change computation"
[5]: https://arxiv.org/abs/2402.19088 "Survey in Characterizing Semantic Change"
[6]: https://www.jmir.org/2025/1/e73950 "Quantifying Mental Health Context and Semantic Severity in Diachronic Corpora"
[7]: https://arxiv.org/abs/2511.09833 "ACT: An Annotator-Critic-Human Correction Framework for LLM-Assisted Annotation"
[8]: https://platform.openai.com/docs/guides/prompt-engineering "OpenAI Prompt Engineering Guide"
[9]: https://docs.anthropic.com/en/docs/claude-code/overview "Claude Code Overview"
[10]: https://aclanthology.org/2022.findings-emnlp.211/ "SetFit: Efficient Few-Shot Learning Without Prompts"
