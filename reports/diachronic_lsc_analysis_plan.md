# SIBling-inspired Diachronic Semantic Change Plan (Adapted)

## 0. Overview

* The study treats lexical semantic change as a continuous annual trajectory rather than as a binary epoch-to-epoch contrast. This follows a McTaggart-style time-series orientation: each measure characterises how target terms move through time by target term, year, and comparator group, instead of reducing change to a single pre/post distance score. ([Cambridge Core][4]; [arXiv][5])
* The conceptual frame follows Baes et al.’s SIBling framework because it treats semantic change as something to characterise, not merely detect: instead of collapsing change into one aggregate vector-drift score, it separates interpretable dimensions of Sentiment, Breadth, and Intensity, here supplemented by Salience and Thematic content. This responds to the broader gap noted in semantic-change research, where detection has received more attention than characterisation. ([ACL Anthology][2]; [arXiv][5])
* The design now includes one supervised **Frame Classification** layer before the five operational LSC measures. Frame Classification distinguishes clinical/disorder framing from identity and lived-experience framing in ADHD/autism contexts, so that later semantic trajectories can be interpreted both overall and within substantively meaningful discourse strata.
* The five operational LSC measures remain: **Salience** as relative mention frequency, **Intensity** as affective arousal/severity of target contexts, **Breadth** as contextual dispersion, **Sentiment** as affective valence of target contexts, and **Thematic evolution** as changing target-neighbour associations over time.
* The main empirical comparison is not simply “ADHD/autism changed” but whether ADHD/autism trajectories exceed or differ from broader background drift in comparator terms.
* Across dimensions, the main analysis treats **ADHD** and **Autism** separately. Raw target forms are aggregated into conceptual target groups for the main estimates, while raw-form diagnostics are retained to check whether one form drives a trajectory.
* Comparator terms are included throughout the analysis as separate baseline series (`frustration`, `sadness`, `loneliness`). A composite baseline may be reported only after inspecting the individual baselines for compatible sample sizes, coverage, and trajectories.
* The design deliberately keeps the measures interpretable: count-based prevalence, lexicon-based VAD indices, target-aware contextual dispersion, and target-neighbour similarity trajectories.
* The first operational change from Baes et al. is that **NRC-VAD v2.1** replaces Warriner et al.’s VAD norms for Intensity and Sentiment because it has wider English coverage, includes common multi-word expressions, and reports human-rated valence, arousal, and dominance scores for more than 55,000 English words and phrases. ([arXiv][1])
* The second operational change is that **XL-LEXEME** replaces a generic sentence embedder for Breadth, because it produces target-aware word-in-context representations rather than general sentence vectors. ([ACL Anthology][3])
* The third operational change is that Thematic evolution will use **pair-wise neighbour-similarity time-series** from diachronic type-level Word2Vec embeddings rather than Baes et al.’s top-down pathologisation dictionary. This follows [Vylomova and Haslam’s target-neighbour method](https://langsci-press.org/catalog/view/303/3028/2375-1) and the later [Neighbours Similarity Evolution adaptation by Iacob and Uban](https://aclanthology.org/2026.lchange-1.12/).
* The fourth operational change is that ADHD/autism analyses will be frame-aware. Pisl et al. show that apparent semantic-severity trends can be explained by changing discourse composition rather than intrinsic semantic change alone; in their case, the time effect for depression became nonsignificant after controlling for mental-health context. This project adapts that insight by making clinical/disorder versus lived-experience framing a core stratum rather than a post-hoc robustness check. ([JMIR][6])

### Shared semantic context contract

For Frame Classification, Sentiment, Intensity, Breadth, and Thematic analyses, the primary annual axis is document publication year (`lsc_year = published_year`), not Common Crawl source/capture year. The shared context table therefore keeps only WARC-validated, English, deduplicated contexts with parseable `published_ts` in the 2014-2026 analysis window. `source_year` remains provenance metadata for crawl-composition diagnostics, not the main diachronic variable.

### Analytic strategy

The downstream analysis keeps close to Baes et al.'s implementation in the parts that define each SIBling dimension: annual collocate-based valence for Sentiment, annual collocate-based arousal for Intensity, annual contextual dispersion for Breadth, relative target frequency for Salience, and thematic collocate/context summaries as an interpretive layer. The main deliberate deviations are the ones stated above: NRC-VAD v2.1 replaces Warriner norms, XL-LEXEME replaces generic sentence embeddings for Breadth, Salience uses the Common Crawl source-year denominator, semantic measures use publication year, and ADHD/autism target contexts are stratified by the locked frame classifier.

For each scalar LSC measure, annual estimates remain the primary objects of interpretation. The notebooks therefore continue to report yearly indices, coverage diagnostics, top contributors, bootstrap intervals where available, and report-ready trajectory figures. To make the trend summaries closer to Baes et al.'s analytical strategy, each scalar notebook also fits compact trend models for the reported annual indices. The main model is an ordinary least-squares regression of the annual index on centred year for each analysis unit and reported stratum. The reported trend table records the slope per year, standard error, p-value, adjusted $R^2$, a standardised year coefficient, and a residual-autocorrelation diagnostic.

Baes et al. additionally use generalised least squares when residual autocorrelation is detected and inspect quadratic forms for some intensity analyses. This project adapts that logic proportionately rather than treating it as a heavy confirmatory time-series framework. With only thirteen annual observations and further frame-stratified target series, OLS trends are treated as descriptive summaries of direction and strength, not causal or population-level tests. A GLS or AR(1)-style sensitivity estimate is added only when the residual diagnostic is flagged, and quadratic year terms are used only as diagnostics for visibly curved trajectories. These choices keep the analysis close to Baes' implementation while avoiding overfitting short frame-year series.

For ADHD and Autism, the main semantic estimates use substantive core discourse only: `clinical_only`, `lived_only`, and `mixed`. These are reported both as separate frame strata and as a `substantive_core_overall` aggregate. `non_substantive_or_insufficient` and `substantive_other` remain visible in composition and quality diagnostics but are excluded from semantic estimates because the former is not a semantic frame and the latter is sparse and heterogeneous. Baseline terms remain unframed and separate. Annual frame-year cells are computed but flagged when they contain fewer than 100 contexts or fewer than 50 documents.

The deliberate deviations from Baes et al. are checked with compact robustness notebooks rather than folded into the main analysis. Sentiment and Intensity are rerun with Warriner norms while keeping the same annual, frame-aware collocate contract; outputs retain both Warriner's native 1-9 scale and a 0-1 scaled score for comparison with NRC-VAD. Breadth is checked with Baes et al.'s `sentence-transformers/all-mpnet-base-v2` sentence encoder while keeping the publication-year, target-frame, mean-pairwise-distance contract fixed. The MPNet breadth check is target-only: ADHD and Autism are retained, comparator terms are omitted because the check is meant to test the encoder deviation rather than re-estimate the full baseline contrast.

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

The final codebook also records four adjudication-derived boundary rules. A target appearing in a list can still be substantive when a coherent claim about the listed category clearly applies to ADHD/autism; concrete institutional provision organised for the target group is substantive clinical/institutional-support discourse; self-theorising symptom language used to interpret everyday experience is lived rather than automatically clinical; and legal/forensic mitigation that invokes the target as an explanatory clinical object is substantive clinical discourse.

### Annotation and classifier workflow

The labels are produced through a human-led, LLM-assisted process. I first used a 200-passage human pilot to develop and refine the codebook. Codex then applied the locked codebook to 3,000 further passages. Gemini independently checks these annotations and ranks the cases that are most likely to contain mistakes. I review the suspicious cases and a random sample of lower-ranked cases before the labels are used for training. Gemini's suggestions are advisory: only human-reviewed decisions can replace the original annotation. This follows the annotator-critic-human-correction logic proposed in ACT while keeping the final correction decision with the human coder. ([arXiv][7])

The random audit matters because a critic can fail to flag some errors. It therefore provides a check on the quality of the annotations that remain unreviewed, rather than assuming that a low critic score means an annotation is correct. Exact model choices, review limits, stopping rules, and execution details are recorded in the design-decision log and runbook rather than repeated here.

The corrected labels are used to train a hierarchical classifier. One component learns whether a passage contains substantive target discourse. Two further components learn whether substantive passages contain clinical framing and lived-experience framing. The latter two components are trained only on substantive passages, so incidental or unusable mentions do not become misleading negative examples.

The final classifier is evaluated once on a separate 200-passage human-coded validation sample. This validation sample is kept out of codebook refinement, LLM annotation, criticism, correction, and model training. If validation performance is adequate, the classifier is applied to the remaining ADHD/autism contexts.

### Outputs

* Protected annotation-ready pilot and validation XLSX workbooks with context fields stored as text and constrained label dropdowns.
* Codebook and locked annotator/critic prompts.
* LLM annotation and criticism batches, validated structured outputs, run metadata, and human-correction handoffs.
* Classifier validation metrics for the hierarchical heads: substantive target discourse on all validation passages, and clinical/lived-experience framing among human-substantive validation passages. Derived-frame predictions remain saved for downstream composition summaries and audits rather than as the headline validation metric.
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

## 2. Measure 1 — Salience (Source-year prominence)

### Concept (Baes et al. framing)

Salience captures how prominent a lexical item or concept becomes in a corpus over time. In Baes et al., this is implemented as normalised target frequency in yearly corpora and used as a complementary measure alongside the main SIBling semantic dimensions. It helps show whether semantic trajectories occur against a background of increasing or decreasing attention to the target terms. ([ACL Anthology][2])

For this project, Salience is the frequency with which ADHD/autism-related expressions appear in annual Common Crawl source-year slices. It answers a different question from the semantic measures: not “what does the term mean?”, but “how often is the term invoked in the sampled web corpus?” The frame-classification results describe the composition of ADHD/autism discourse separately; they are not folded into the Salience measure.

Unlike Frame Classification, Sentiment, Intensity, Breadth, and Thematic analyses, Salience uses Common Crawl source year as its primary annual axis. This is a deliberate denominator choice: the available denominator is the yearly WET sample scanned for target and baseline terms. Publication-year composition is retained as a caveat table, but publication year is not used as the Salience denominator because documents without term hits do not receive WARC extraction or publication-date recovery.

### Operational definition (my pipeline)

For each analysis unit $u$ and Common Crawl source year $Y$, the primary Salience series is WARC-validated hits per million minimum-length WET tokens:

$$
\text{Salience}_{u,Y}
=
1{,}000{,}000
\times
\frac{H^{\text{WARC}}_{u,Y}}{T^{\text{WET}}_Y}
$$

where $H^{\text{WARC}}_{u,Y}$ is the number of WARC-validated hits for analysis unit $u$ in source year $Y$, and $T^{\text{WET}}_Y$ is the annual token count for minimum-length WET documents entering the term scan.

This stays close to Baes et al.’s normalised target-frequency logic while adapting it to the collection design. WARC validation supplies the numerator used for the reported trend, and the WET scan supplies a stable source-year denominator. Candidate and WET-stage counts are retained only as audit checks on the validation contract.

Analysis units:

* ADHD: `adhd`, `attention[-\s]?deficit`
* Autism: `autism`, `autistic`, `autism[-\s]?spectrum`, and disambiguated `ASD`
* Baselines: `frustration`, `sadness`, `loneliness`

### High-level implementation plan (step-by-step; no code)

#### Inputs needed

* Processed Common Crawl trend output with annual scan denominators and validation-stage counts.
* WARC-validated target and comparator hit counts by source year, analysis unit, and raw form.
* Publication-year status summaries for WARC-validated hits, used only to document the source-year caveat.

#### Preprocessing assumptions

* One deterministic crawl slice per year.
* Same scan logic, document-length threshold, target regexes, ASD disambiguation rule, and domain cap across all years.
* Target and comparator terms pass through the same WET scan and WARC validation contract.
* The source-year WET token denominator is the appropriate denominator for this measure because it covers the full scanned yearly corpus, not only extracted hit documents.

#### Computation steps

1. Check that annual source-year coverage, denominators, and validation-stage ordering are complete for 2014-2026.
2. Aggregate WARC-validated hits by source year, analysis unit, and raw form.
3. Compute WARC-validated hits per million WET tokens for ADHD, autism, and each comparator term.
4. Add a 2014-indexed value for each analysis unit to compare proportional movement across terms with different absolute frequencies.
5. Fit compact annual trend summaries: OLS Salience on centred source year, with residual-autocorrelation and simple nonlinearity diagnostics.
6. Write publication-year and raw-form caveat tables so that source-year interpretation and raw-form balance remain inspectable without adding extra report figures.

#### Outputs (tables/figures)

* `salience_unit_year.csv`: annual source-year Salience table, including absolute rates and 2014-indexed proportional movement.
* `salience_trend_summary.csv` and `salience_trend_models.csv`: descriptive trend summaries and compact regression outputs.
* `salience_audit_checks.csv` and `salience_denominator_audit.csv`: validation-contract and denominator diagnostics.
* `salience_raw_form_year.csv`: raw-form composition by year for ADHD and autism.
* `salience_publication_year_status.csv` and `salience_publication_year_document_status.csv`: caveat tables showing how source-year hits relate to recovered publication years.
* `lsc_salience_primary_trajectories.{png,pdf}`: one two-panel report figure. The left panel shows absolute ADHD/autism WARC-validated hits per million WET tokens; the right panel indexes each target and comparator term to 2014 to show proportional movement.

#### Diagnostics / sanity checks

* Check that each source year has the expected scan denominator and validation-stage counts.
* Inspect raw-form composition so that ADHD/autism trends are not silently driven by one spelling, acronym, or expansion.
* Inspect publication-year status to make clear how much of the source-year trend comes from pages with recovered in-window publication dates.
* Compare ADHD/autism proportional movement with the three comparator terms in the indexed panel, while keeping the absolute ADHD/autism rates as the main Salience result.

#### Efficiency notes

Salience is cheap once the processed trend handoff exists. The main efficiency concern is not computation but reproducibility: source-year denominators, validation-stage definitions, raw-form grouping, and the source-year/publication-year distinction must stay explicit.

### Key assumptions + likely failure modes

* **Assumption:** the annual Common Crawl slices are comparable enough to support relative-frequency trends.
* **Assumption:** WARC validation removes enough boilerplate to make the salience signal substantively interpretable.
* **Failure mode:** crawl composition changes masquerade as discourse change.
* **Failure mode:** source-year prominence is mistaken for publication-year prevalence or public concern.
* **Failure mode:** ADHD/autism terms appear in navigation, accessibility widgets, product pages, or directory pages rather than substantive prose.
* **Failure mode:** the 2014-indexed panel is overinterpreted as the main measure. It is only a proportional comparison aid; the absolute target rates and trend tables remain the Salience result.

## 3. Measure 2 — Intensity (Vertical creep)

### Concept (Baes et al. framing)

Intensity corresponds to whether a term’s usage shifts toward more or less emotionally intense contexts. In concept-creep terms, vertical creep refers to harm- or pathology-related concepts extending to less severe phenomena. Baes et al. operationalise this dimension through affective arousal of collocates. ([ACL Anthology][2])

For this project, Intensity is an annual measure of how emotionally activated or severity-laden the local contexts of ADHD/autism terms are. A falling arousal/severity trajectory may be consistent with vertical creep, but it is not sufficient evidence on its own. It must be interpreted alongside Breadth, Sentiment, thematic context, and the clinical/lived-experience frame composition of target contexts.

### Operational definition (my pipeline)

For each analysis unit and year:

* extract ±5-word collocate windows around target mentions;
* match collocates to **NRC-VAD v2.1**;
* compute an annual weighted mean **arousal** score.

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

### High-level implementation plan (step-by-step; no code)

#### Inputs needed

* WARC-extracted, quality-gated documents.
* Target mention table with document ID, year, target group, raw target form, sentence/context location.
* Tokenised and optionally lemmatised text.
* NRC-VAD v2.1 lexicon.

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

8. Model the annual trajectory with simple trend summaries and plots, not just first-versus-last comparisons.

#### Outputs (tables/figures)

* Annual arousal index by target group and comparator.
* Coverage table by year and target group.
* Top arousal-contributing collocates per decade or year group.
* Line plot of annual arousal trajectories.
* Bootstrap confidence intervals for annual scores if sample sizes permit.

#### Diagnostics / sanity checks

* Inspect years with unusually low VAD coverage.
* Check whether high-arousal scores are driven by a small number of repeated collocates.
* Compare ADHD/autism trajectories to comparator trajectories.
* Manually inspect high- and low-arousal windows for face validity.
* Check whether arousal changes are genre-driven, e.g. news/crime pages versus support/information pages.
* Separate ADHD and autism before aggregating, because they may have different intensity trajectories.

#### Efficiency notes for ~1,000–1,500 docs per term per year

The collocate-based VAD calculation is computationally light. The main efficiency concern is preserving a reusable collocate handoff so Sentiment and Intensity do not duplicate preprocessing.

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

* use all markable ADHD/autism target contexts in the substantive core frame strata and in the duplicated `substantive_core_overall` aggregate;
* use deterministic domain-stratified samples capped at 1,000 contexts per baseline-term year;
* mark the target span;
* encode the marked contexts using **XL-LEXEME**;
* compute contextual dispersion as average pairwise cosine distance within each unit-year-frame sample.

Formally, for analysis unit $u$ in year $Y$ and reported stratum $s$, with $N_{u,Y,s}$ sampled target-containing contexts and XL-LEXEME vectors $\mathbf{v}_1, \ldots, \mathbf{v}_{N_{u,Y,s}}$:

$$
\text{Breadth}_{u,Y,s}
=
\frac{2}{N_{u,Y,s}(N_{u,Y,s}-1)}
\sum_{i=1}^{N_{u,Y,s}-1}
\sum_{j=i+1}^{N_{u,Y,s}}
\left(
1 -
\frac{\mathbf{v}_i \cdot \mathbf{v}_j}
{\lVert \mathbf{v}_i \rVert \lVert \mathbf{v}_j \rVert}
\right)
$$

Higher values indicate greater average contextual dissimilarity among target uses in that year and reported stratum.

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
* Fixed sampling parameters per target group, frame stratum, baseline term, and year.

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
3. Keep all markable ADHD/autism target rows for each target-year-frame stratum. If more than 1,000 baseline contexts are available for a baseline-year, use a deterministic domain-stratified random sample.
4. Mark the target span in each sentence.
5. Encode each marked sentence with XL-LEXEME.
6. Compute annual contextual dispersion from L2-normalised embeddings, using the closed-form mean-pairwise cosine distance rather than materialising all pairwise distances except for diagnostics.
7. Average distances to obtain the annual Breadth score.
8. Repeat sampling several times or bootstrap contexts to obtain uncertainty intervals.
9. Produce annual trajectories for ADHD, autism overall substantive-core usage, ADHD/autism core frame strata, and comparator terms.
10. Optionally compute adjacent-year or anchor-period distances as auxiliary diagnostics, but keep the primary Breadth measure as within-year contextual dispersion.

#### Outputs (tables/figures)

* Annual Breadth score by target group, frame stratum, and comparator.
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

A practical MSc configuration is therefore to use all available target contexts, cap only high-volume baseline comparator cells, store embeddings for reuse, and compute uncertainty from resampling over documents where possible. GPU inference is preferable, but batched CPU inference may be acceptable if the long Breadth notebook is run separately rather than inside an interactive planning session.

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

### Concept

Baes et al. treat thematic content as a complementary interpretive layer: it identifies what kinds of contexts or themes surround the target term. Their implementation uses a top-down pathologisation dictionary. ([ACL Anthology][2])

This project uses a bottom-up embedding approach instead, because ADHD/autism discourse is not limited to pathologisation. The method follows [Vylomova and Haslam’s pair-wise similarity time-series of type-level embeddings](https://langsci-press.org/catalog/view/303/3028/2375-1) and the later [Neighbours Similarity Evolution adaptation by Iacob and Uban](https://aclanthology.org/2026.lchange-1.12/). It asks which content words become more or less distributionally close to the target concept over time.

Thematic evolution is therefore qualitative-quantitative rather than a fifth scalar index. The main outputs are annual top-neighbour tables and report figures showing the cosine-similarity trajectories between each target concept and its most persistent neighbours.

### Operational definition

For each target group and frame stratum, train diachronic type-level Word2Vec embeddings on target-centred substantive passages:

1. canonicalise all ADHD forms to `adhd_concept` and all autism/autistic/ASD forms to `autism_concept`;
2. lemmatise and content-filter the target-centred passages;
3. train one global skip-gram Word2Vec model for the target-frame corpus;
4. initialise annual Word2Vec models from the global model and continue training each annual model on that year’s passages;
5. retrieve the annual top five eligible neighbours of the canonical target token;
6. choose report-facing neighbours from the annual top-neighbour lists only when they are stable enough for a trajectory plot;
7. plot the annual cosine similarity between the target token and each selected stable neighbour.

Formally, for target concept $w_i$, neighbour $w_j$, and publication year $t$:

$$
s^{(t)}(w_i, w_j) = \\cos(\\mathbf{w}^{(t)}_i, \\mathbf{w}^{(t)}_j)
$$

The model uses publication year (`lsc_year`), not Common Crawl source year. ADHD and Autism are modelled separately. The report-facing strata are `substantive_core_overall`, `clinical_only`, and `lived_only`; mixed-frame contexts contribute to the Overall model but remain out of the compact figures.

### High-level implementation plan

#### Inputs needed

* Shared LSC target-context table with `target_sentence_plus_adjacent`, `lsc_year`, raw target form, document, and domain metadata.
* Locked frame-label handoff for ADHD and Autism target contexts.
* spaCy English lemmatisation.
* Gensim Word2Vec.

#### Preprocessing assumptions

* Use target-centred passages rather than full hit documents so the analysis remains target-specific and frame-aware.
* Use only substantive core target contexts: `clinical_only`, `lived_only`, and `mixed`.
* Canonical target tokens are necessary because type-level Word2Vec otherwise splits the conceptual target across raw forms such as `ADHD`, `attention deficit`, `autism`, `autistic`, and `ASD`.
* Remove punctuation, numerals, stopwords, one-character tokens, and generic web artifacts before training; preserve content words that can function as interpretable thematic neighbours.

#### Computation steps

1. Merge target contexts with frame labels and inspect input diagnostics by target, year, and predicted frame.
2. Canonicalise target forms, lemmatise target-centred passages, and inspect token diagnostics.
3. Build six modelling corpora: ADHD and Autism each for Overall, clinical-only, and lived-only.
4. Train or load cached Word2Vec models for each target-frame corpus using skip-gram, window size 10, minimum corpus count 5, 200 dimensions, and 10 global plus 10 annual epochs.
5. Extract annual top-five neighbours for each target-frame-year model.
6. Select report neighbours automatically from the annual top-five lists by years seen, mean similarity, best rank, and lexical tie-break, requiring at least two top-five appearances and at least ten finite trajectory years.
7. Export the annual top-neighbour table, stable plotted-neighbour table, stable neighbour trajectories, and an execution summary.
8. Save one report figure for ADHD and one for Autism, each with three equal-width panels: Overall, Clinical/disorder, and Lived experience; save exploratory appendix heatmaps for annual top-neighbour churn.

#### Outputs

* `lsc_thematic_tokenised_contexts.parquet`: tokenised target-context handoff.
* Cached Word2Vec models under `data/interim/lsc/thematic_evolution/word2vec_models/`.
* `lsc_thematic_annual_top_neighbours.csv`.
* `lsc_thematic_plotted_neighbours.csv`.
* `lsc_thematic_neighbour_similarity_trajectories.csv`.
* `lsc_thematic_execution_summary.json`.
* `lsc_thematic_neighbour_similarity_adhd.{png,pdf}` and `lsc_thematic_neighbour_similarity_autism.{png,pdf}`.
* `appendix/lsc_thematic_neighbour_rank_heatmap_adhd.{png,pdf}` and `appendix/lsc_thematic_neighbour_rank_heatmap_autism.{png,pdf}`.

Input, token, model, training, audit, and mixed-frame contribution diagnostics are displayed compactly in the notebook rather than exported as separate processed CSV files.

#### Diagnostics / sanity checks

* Check that every target-frame-year model has enough canonical target-token evidence.
* Check that annual top-neighbour lists are complete.
* Inspect token diagnostics so the content filter has not removed the target token or collapsed the vocabulary too aggressively.
* Inspect annual top-neighbour tables alongside the figures; the line plots are summaries of stable automatically selected neighbours, not topic labels.
* Treat descriptive slopes for neighbour trajectories as reading aids, not confirmatory trend models.

### Key assumptions + likely failure modes

* **Assumption:** target-centred passages provide enough local lexical context for type-level Word2Vec neighbour analysis.
* **Assumption:** canonical concept tokens make the target representation more faithful than raw-form-specific embeddings.
* **Assumption:** annual target-frame corpora are large enough for stable neighbour lists after content filtering.
* **Failure mode:** neighbours may reflect web genre or extraction artifacts rather than substantive discourse if boilerplate survives filtering.
* **Failure mode:** year-specific models initialised from a global model may smooth away some abrupt annual changes.
* **Failure mode:** sparse neighbours can appear or disappear across years, so missing trajectory points must not be overinterpreted.
* **Failure mode:** automatic top-neighbour selection improves reproducibility but may select less interpretable neighbours than manual curation.

### Optional enhancements

* **Optional:** add a full-hit-document Overall-only sensitivity model if target-centred passages are challenged as too narrow.
* **Optional:** add a manually curated figure variant from the saved top-neighbour tables if the automatic top-five neighbours are too generic for exposition.
* **Optional:** compare hard frame labels with probability-weighted frame membership only if classifier uncertainty becomes central to interpretation.


[1]: https://arxiv.org/abs/2503.23547?utm_source=chatgpt.com "NRC VAD Lexicon v2: Norms for Valence, Arousal, and Dominance for over 55k English Terms"
[2]: https://aclanthology.org/2024.acl-long.76/ "A Multidimensional Framework for Evaluating Lexical Semantic Change with Social Science Applications"
[3]: https://aclanthology.org/2023.acl-short.135/ "XL-LEXEME: WiC Pretrained Model for Cross-Lingual LEXical sEMantic changE"
[4]: https://www.cambridge.org/core/journals/natural-language-engineering/article/stateoftheart-of-semantic-change-computation/CCD69C7C2306B0E4D246B456E236EFAF "A state-of-the-art of semantic change computation"
[5]: https://arxiv.org/abs/2402.19088 "Survey in Characterizing Semantic Change"
[6]: https://www.jmir.org/2025/1/e73950 "Quantifying Mental Health Context and Semantic Severity in Diachronic Corpora"
[7]: https://arxiv.org/abs/2511.09833 "ACT: An Annotator-Critic-Human Correction Framework for LLM-Assisted Annotation"
[8]: https://aclanthology.org/W19-4704/ "Semantic Changes in Harm-Related Concepts in Psychology"
[9]: https://aclanthology.org/2026.lchange-1.12/ "Concept Creep and Psychology in Social Media and Scientific Literature"
