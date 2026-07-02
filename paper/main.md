<div class="titlepage">

**Tracing Semantic Change in ADHD and Autism Discourse at Web Scale**

Jakob Lütkemeier

MSc Dissertation submitted in partial fulfilment of the requirements for the degree of

MSc in Applied Social Data Science

[School of Social Sciences and Philosophy](https://www.tcd.ie/ssp/)

[Department of Political Science](https://www.tcd.ie/Political_Science/)

Supervisor: Dr Tom Paskhalis

Trinity College Dublin

10 August 2026

*Note: Student ID is intentionally not printed (university guidance).*

</div>

## Declaration

I hereby declare that this MSc Dissertation is entirely my own work and that it has not been submitted as an exercise for a degree at this or any other university.

I have read and I understand the plagiarism provisions in the General Regulations of the University Calendar for the current year, found at <http://www.tcd.ie/calendar>.

I have also completed the Online Tutorial on avoiding plagiarism “Ready Steady Write”, located at <http://tcd-ie.libguides.com/plagiarism/ready-steady-write>.

Signed: 

------------------------------------------------------------------------

Date: 

------------------------------------------------------------------------

# Abstract

\[Provisional\] This dissertation investigates whether “therapy-speak” related to ADHD and autism has increased in general web discourse over the last decade, and how the surrounding language and framing may have shifted over time. Using Common Crawl as a large-scale web archive, I implement a reproducible pipeline that samples one crawl per year and extracts plaintext (WET) documents for trend estimation and a smaller, deeper corpus for downstream NLP analysis.

## Acknowledgements

I would like to thank my supervisor, Dr Tom Paskhalis, for guidance throughout this dissertation, and the MSc in Applied Social Data Science teaching team for methodological training and support.

# Introduction

Reported rates of ADHD (Cui et al. 2026; McKechnie et al. 2023) and autism (Zeidan et al. 2022) have risen sharply. Concurrently, mental health awareness campaigns have proliferated (Bolinski et al. 2020), and mental health-related discourse has become increasingly common in everyday life (Foulkes and Andrews 2023), giving rise to what has been described as *therapy-speak*. Therapy-speak refers to the imprecise and superficial integration of psychotherapy language into everyday communication (Isern-Mas and Almagro 2025).

This concurrence has led some scholars to question the temporal relationship between rising prevalence rates and the increasing everyday use of mental health language. Foulkes and Andrews (2023) describe this as the *Prevalence Inflation Hypothesis*, which proposes a cyclical, intensifying relationship between the two phenomena. On one side of this cycle, rising prevalence rates may understandably increase mental health-related discourse in everyday life, hereafter referred to as therapy-speak. On the other side, the spread of therapy-speak may itself contribute to further increases in prevalence rates through overdiagnosis and overinterpretation (Foulkes and Andrews 2023).

Before considering the potential risks of therapy-speak, its positive effects should be emphasised. The popularisation of therapy-speak may give people language for subtle emotional states (Medaris 2024), improve mental health literacy (Schomerus et al. 2012), and thereby reduce stigma around mental illness (Sampogna et al. 2017; Fleary et al. 2022). It may also support better recognition and more accurate reporting of mental health problems (Foulkes and Andrews 2023) and facilitate online identity and community formation, particularly around ADHD and autism (Ginapp et al. 2023).

At the same time, therapy-speak carries several risks that are predominantly epistemic in nature. These risks are not mutually exclusive and may overlap. First, therapy-speak may encourage overinterpretation, whereby milder and more transient forms of distress are conflated with mental health problems, often in conjunction with self-diagnosis (Foulkes and Andrews 2023). At best, this may alter self-concept and behaviour (Foulkes and Andrews 2023; Alper et al. 2025). At worst, when an individual interprets and labels their psychological experiences as a mental health problem, this may bring those symptoms into existence in the manner of a self-fulfilling prophecy. Adolescents may be particularly vulnerable to these dynamics because they are more prone to rumination, peer influence, and media messaging (Foulkes and Andrews 2023).

Second, therapy-speak may contribute to the psychiatrisation of everyday suffering and distress (Brinkmann, 2014; Beeker et al., 2024). Third, mental health problems may be glamorised or romanticised, particularly among adolescents on social media (Ndour and Foulkes 2025). Fourth, therapy-speak may spread inaccurate psychological information, including inaccurate information about autism (Aragon-Guevara et al. 2025) and ADHD (<span class="nocase">de Vries et al.</span> 2025). Finally, therapy-speak may erode the meaning and relevance of mental-health-related terms, a process that can also be understood as trivialisation. This can contribute to hermeneutical injustice by depriving people who actually live with a certain mental condition of the words they need to describe their experiences (Isern-Mas and Almagro 2025) and by reducing their symptoms to mere personality traits, thereby denying them a fully recognised psychiatric identity (Spencer and Carel 2021).

This latter concern—the erosion of meaning in psychotherapy terms—is the focus of the present study, which investigates diachronic lexical semantic change in the terms ADHD and autism. These two terms stand out, arguably more than any others, when considering psychotherapy-related terms that have permeated mainstream discourse (Medaris 2024). From January to May 2024, ADHD was the subject of 25,080 media articles, compared with 5775 articles during the equivalent period in 2014 (Martin et al. 2025). In May 2026, adhd and autism had been hashtagged in more than 5.2 million and 3.9 million videos on TikTok, respectively.

The following related work chapter first synthesises empirical evidence on lexical semantic change (LSC) in mental-health-related terms over recent decades, before reviewing the computational approaches used to study these processes and identifying the gap addressed by this project.

# Related Work

## Lexical Semantic Change of Mental-Health-Related Concepts

Lexical semantic change (LSC) refers to shifts in a word’s meaning while its grammatical function remains stable, and constitutes a common form of language change (Campbell 2013). For example, cloud, initially a meteorological term, broadened in usage to refer to internet-based data storage.

*Concept Creep Theory* posits that harm-related concepts are particularly prone to LSC. Many harm-related terms, including addiction, bullying, harassment, prejudice, and trauma, appear to have expanded in meaning since at least the 1970s (Haslam 2016; Haslam et al. 2020). Concept creep distinguishes between two forms of LSC that can occur concurrently: vertical creep and horizontal creep. Vertical creep refers to the loosening of definitions to include milder instances, whereas horizontal creep refers to the extension of definitions to encompass qualitatively new phenomena.

Previous studies have examined concept creep across different mental-health-related concepts. Vylomova and Haslam (2021) found that many of the terms they studied, including addiction, bullying, prejudice, harassment, and trauma, displayed both vertical creep and horizontal creep in a corpus of 825,628 psychology article abstracts from 875 journals and, to a lesser extent, in a general corpus combining the Corpus of Contemporary American English (CoCA) and the Corpus of Historical American English (CoHA). Baes, Vylomova, et al. (2023) found evidence of vertical creep in the word trauma in Vylomova and Haslam’s psychology corpus. Baes, Haslam, et al. (2023) similarly found vertical creep in mental-health-related concepts in the same psychology corpus, including addiction, anger, stress, and worry, as well as in Vylomova and Haslam’s general corpus, including addiction, grief, stress, and worry.

Not all findings have aligned with this pattern. Xiao et al. (2023) reported that the average severity of collocates for both anxiety and depression *increased* in both Vylomova and Haslam’s psychology and general corpora, contrary to expectation. This unexpected result may reflect measurement unspecificity, particularly the absence of discourse context and construct distinctions. For example, depression may refer to psychiatric depression, but also to meteorological or economic phenomena. Similarly, anxiety may refer to a nosological category, a disorder construct, or an underlying human experience. Indeed, Pisl et al. (2025) showed that when psychiatric depression and anxiety as human experience were isolated, both displayed vertical creep. Iacob and Uban (2026) examined a range of common therapy-speak terms, including toxic, bipolar, psychopath, narcissistic, and triggered, and found mixed results. In an extension of Vylomova and Haslam’s psychology corpus, these terms displayed horizontal creep. However, in a Reddit corpus comprising general Reddit comments and comments from psychology-oriented subreddits, results were mixed.

On balance, most mental-health-related concepts appear to have become milder and broader over the last few decades, although evidence for broadening is weaker.

### Factors Influencing Concept Creep of Mental-Health-Related Terms

As noted above, general LSC is ubiquitous and naturally-occuring to some extent (Campbell 2013). Frequency and polysemy are said to explain most variance in rates of lexical semantic change (Hamilton et al. 2016). However, the causes of the disproportionate levels of LSC observed in harm-related concepts remain uncertain.

Proposed explanations include cultural shifts toward greater sensitivity to harm, postmaterialist values, and diminished exposure to adversity (Haslam et al. 2020; Furedi 2016). In the case of mental-health-related terms, evidence is unclear as to whether the “creeping” of mental-health-related concepts reflects changes in official diagnostic criteria, broader sociocultural factors, or a combination of both.

Fabiano and Haslam (2020) showed that no revision of the Diagnostic and Statistical Manual of Mental Disorders (DSM) from the third edition onward was reliably more inflationary or deflationary overall. However, specific disorders changed significantly. Most notably, ADHD inflated by 18%, 33%, and then 17% in the three revisions following DSM-III. Rates of autism also inflated by 50% from DSM-III to DSM-III-R, albeit based on a single study, but deflated by 15% from DSM-IV to DSM-5.

## Computational Approaches to Studying Diachronic Lexical Semantic Change

Since 2018, advances in deep learning have expanded the methodological repertoire for modelling semantic change (Manning 2022). In particular, they have supported the development of language models that encode words as increasingly sophisticated vector representations. This marks a shift from count-based approaches, where word meaning is inferred from patterns of co-occurrence, toward prediction-based embeddings, where vectors are learned iteratively through a language-modelling objective (Grimmer et al. 2022; Jurafsky and Martin 2026).

Building on these methodological developments, Baes et al. (2024) proposed the *SIBling* (**S**entiment, **I**ntensity, and **B**readth) framework as a unified and multidimensional approach to LSC. The framework situates the notion of concept creep within a broader account of LSC and links this conceptual model to computational methods, including both count-based and embedding-based techniques. It distinguishes at least three dimensions along which terms may change over time: vertical drift, horizontal drift, and sentiment.

Vertical drift refers to changes in severity or intensity. A term may become stronger, as in hilarious shifting from cheerful or amusing to extremely funny, or weaker, as in trauma shifting from brain injuries to milder events such as business loss. This dimension maps onto vertical concept creep. Horizontal drift refers to changes in breadth. A term may become narrower, as in doctor shifting from scholar or teacher to primarily denoting a medical professional, or wider, as in cloud shifting from a meteorological term to internet-based data storage. This dimension maps onto horizontal concept creep. Sentiment refers to changes in connotation. A term may acquire a more positive connotation, as in geek shifting from a derogatory term for odd people to someone passionate about a field, or a more negative connotation, as in retarded shifting from a neutral term for intellectual disability to a highly pejorative term. This dimension is roughly equivalent to destigmatisation and stigmatisation.

According to SIBling, these dimensions can be complemented by evaluating shifts in the frequency of target words and in the thematic content of their collocates.

## Empirical Gap

Two gaps motivate the present study. The first concerns the evidentiary basis of existing work. Research on lexical semantic change in mental-health-related concepts has relied heavily on curated—and often the same few—corpora. Many studies of general language have used Vylomova and Haslam (2021) combined CoCA and CoHA corpus (Baes, Haslam, et al. 2023, 2024; Xiao et al. 2023), while a large number of studies of psychology-specific language have repeatedly drawn on Vylomova and Haslam (2021) psychology corpus (Baes et al. 2024, 2023; Xiao et al. 2023; Iacob and Uban 2026). Other analyses have turned to Reddit data (Kang et al. 2025; Iacob and Uban 2026).

These corpus choices are consequential because the broader objective is to infer cultural dynamics from lexical change. Curated corpora may reflect shifts in editorial policy, audience orientation, or ideological stance rather than wider changes in public discourse (Pisl et al. 2025). Reddit, similarly, is not a neutral proxy for general discourse, as its user base, like that of many social media platforms, is demographically skewed (Gjurković et al. 2021). To date, research on lexical semantic change in mental-health-related concepts has not examined general public web discourse.

The second gap concerns the target concepts themselves. Direct evidence on lexical semantic change in ADHD and autism remains sparse, although Kang et al. (2025) showed that ADHD and autism appear to have converged on Reddit. From 2019 onward, their semantic similarity increased, with ADHD and autism becoming more contextually similar than comparison conditions. Research into diachronic LSC of these two terms independently remains wanting.

The present study addresses these two gaps.

## The Current Study

The present study is guided by three research questions: (i) how has the prevalence of ADHD- and autism-related terminology in general web discourse changed from 2014 to 2026; (ii) how have the semantic profiles of ADHD and autism evolved over this period, relative to non-clinical negative baseline emotion terms; and (iii) how has the thematic framing of ADHD and autism changed over time in public web discourse?

The web data are collected using Common Crawl. We built an economical and reproducible pipeline to extract quality-gated and substantive web content around ADHD and autism, as well as baseline terms.

Methodologically, the study adopts Baes et al. (2024) SIBling Framework with two deliberate deviations. First, for measuring intensity and sentiment, the study uses Mohammad (2025) NRC-VAD Lexicon rather than Warriner norms. This dictionary contains more terms, 20,007 compared with 13,915, and has been shown to be substantially more reliable, with reliability of 0.923 compared with 0.823 aggregated across three dimensions (Mohammad 2025). Second, instead of SIBling’s top-down, dictionary-based thematic index geared toward measuring pathologisation, the study employs a bottom-up topic-modelling approach to capture emergent framings of the target terms.

In all, this project contributes by extending concept creep and therapy-speak research to ADHD and autism; shifting evidence from platform-specific (Iacob and Uban 2026; Kang et al. 2025) or curated corpora (Baes et al. 2024, 2023; Baes, Vylomova, et al. 2023; Xiao et al. 2023) toward general web discourse; implementing and making available an economical, reproducible Common Crawl pipeline designed to extract high-quality general discourse to study diachronic lexical-semantic change in target terms against matched baseline terms; and validating and extending the SIBling framework.

Consistent with the gravity of the literature (Baes, Vylomova, et al. 2023; Baes, Haslam, et al. 2023; Xiao et al. 2023; Iacob and Uban 2026; Vylomova and Haslam 2021), we hypothesise that ADHD and autism have become more frequent and their collocates milder and broader since 2014. Given the absence of prior findings, the present study adopts a conservative approach and proposes no direction regarding changes in sentiment or thematic content among terms collocating with the target terms.

# Data and Materials

## Common Crawl

Common Crawl is the largest freely available public archive of web crawl data and is released in recurring crawl snapshots (Baack 2024; Common Crawl Team 2024). It is widely used as a web-scale source of language data for search, corpus construction, NLP research, and large language model pretraining (Baack 2024; Wenzek et al. 2019). For this project, its central value lies in breadth: Common Crawl provides repeated cross-sections of general web discourse rather than data from a single platform, newspaper archive, or curated clinical corpus. However, it should not be treated as a representative survey of the web or of public opinion. The resulting data reflect what Common Crawl crawled, retained, and made available.

<figure id="fig:commoncrawl-pipeline" data-latex-placement="htbp">
<embed src="commoncrawl_pipeline/commoncrawl_collection_pipeline.pdf" />
<figcaption>Common Crawl collection pipeline used to construct the trend and corpus materials.</figcaption>
</figure>

To collect the data for this study, we built a Common Crawl collection pipeline designed to extract high-quality general discourse for the analysis of diachronic lexical-semantic change in specific target terms, here ADHD and autism, against matched baseline terms. The collection workflow is illustrated in Figure <a href="#fig:commoncrawl-pipeline" data-reference-type="ref" data-reference="fig:commoncrawl-pipeline">3.1</a>. Target and baseline terms are processed together so that yearly denominators, sampling logic, and quality filters remain comparable across term groups. The pipeline uses Common Crawl’s two main file formats sequentially. WET files provide compact extracted plaintext and are therefore used for large-scale term scanning and yearly prevalence denominators, but they lack the HTML structure and metadata needed for stronger validation. Candidate documents are therefore resolved to their corresponding WARC records, which preserve the archived web response, including HTML and headers. Although WARC processing is slower, it enables main-text extraction, term-survival validation, and metadata recovery. This WET-first, WARC-second design keeps the pipeline economical by reserving expensive WARC processing for candidate documents only.

The design consists of two linked tracks. The trend track uses fixed-effort annual samples to estimate how frequently target and baseline terms appear over time. The corpus track builds a larger, quality-gated document corpus for downstream NLP analysis. To reduce the risk that a small number of large websites dominate the corpus, domain caps are applied at 50 WET-validated hit rows per registered domain per Common Crawl crawl. Intermediate summaries and manifests are retained so that each year, crawl, track, and batch remains auditable. The pipeline is designed to run end-to-end on AWS EC2, using S3 as the durable storage and transfer layer for intermediate and final collection artefacts. Yearly crawl selection is deterministic: one Common Crawl snapshot is selected per year near a fixed annual anchor date and then frozen in a crawl map.

Several software choices are methodologically consequential because they affect corpus membership. WET and WARC records are parsed with `warcio`, WARC pointers are resolved through a local `pywb`-based index server, archived HTML is converted to main text with Trafilatura and Resiliparse, and post-extraction filtering uses DataTrove quality filters followed by English-language filtering with `py3langid`.

The data collection spans 13 annual Common Crawl snapshots from 2014 to 2026. In the trend track, the pipeline scanned 27.7 million WET records and retained 78,899 WARC-validated term hits. In the corpus track, it scanned 110.0 million WET records, produced 315,410 WARC-validated hits, and retained 167,520 analysis-ready documents after quality filtering, English-language filtering, and near-duplicate removal. Target-term coverage comprises 43,379 target documents, with group-level counts of 15,620 ADHD documents and 33,675 autism documents. These group counts are not mutually exclusive because a document can contain terms from both target groups. The collection was run on an AWS `m7i-flex.large` instance, featuring 2 vCPUs, 8 GiB of RAM, and up to 12.5 Gbps network bandwidth (AWS, n.d.). Available timing summaries for the corpus track cover 95.4 million scanned WET records and imply approximately 0.93 hours per million scanned WET records on this instance type.

### Preprocessing

\[placeholder; leave blank for now\]

## Target Terms

Two target concepts were selected for diachronic lexical semantic change analysis: *ADHD* and *autism*. Target documents were retrieved using the matching expressions shown in Table <a href="#tab:target-patterns" data-reference-type="ref" data-reference="tab:target-patterns">3.1</a>. The abbreviation `ASD` was retained only when *autism* occurred within $`\pm`$<!-- -->200 characters, reducing false positives from unrelated acronym use.

<div id="tab:target-patterns">

| Concept | Matching expressions             |
|:--------|:---------------------------------|
| ADHD    | `̱`; `attention[-]?deficit`       |
| Autism  | `̱`; `̱`; `autism[-]?spectrum`; `̱` |

Target-term matching expressions.

</div>

For comparison, I selected three negative, non-clinical emotion terms with sufficient coverage and interpretable usage: *frustration*, *sadness*, and *loneliness*. These terms are not exact semantic controls for ADHD and autism; they provide a baseline for separating target-specific change from broader shifts in negative affective language in the corpus.

## NRC–VAD Lexicon

The affective analyses use the NRC Valence, Arousal, and Dominance (VAD) Lexicon v2.1 (Mohammad 2025). The lexicon provides real-valued ratings for approximately 55,000 English words and phrases. Scores range from 0 to 1, where higher values indicate greater valence, arousal, or dominance.

This study uses valence to estimate whether target-term contexts become more positive or negative over time, and arousal to estimate changes in emotional intensity. Dominance is present in the source lexicon but is not used. The ratings were produced through Best–Worst Scaling, where annotators are given four items (4-tuple) and asked which item is the Best (highest in terms of the property of interest) and which is the Worst (least in terms of the property of interest).

# Methods

## Analytic Overview

The analysis adapts the SIBling framework of Baes et al. (2024), which characterises lexical semantic change along interpretable dimensions rather than treating change as a single aggregate distance. The study estimates five annual trajectories for ADHD and autism discourse: salience, intensity, breadth, sentiment, and thematic content. Salience measures how often target and baseline terms occur in sampled Common Crawl slices. Intensity and sentiment measure the affective arousal and valence of local collocates. Breadth measures contextual dispersion among target-aware embeddings. Thematic evolution identifies the substantive topics that organise target-term contexts over time.

ADHD and autism are analysed as separate conceptual target groups throughout. The raw forms that instantiate each group are retained for diagnostics, but the main estimates are group-level trajectories. The comparator terms *frustration*, *sadness*, and *loneliness* remain separate baseline series rather than being collapsed into a composite baseline. This preserves visibility over broader changes in negative affective language and avoids imposing a single background series unless the comparator trajectories are empirically compatible.

The annual time axis differs by measurement task. Salience uses Common Crawl source year because its denominator is the annual WET scan: documents without candidate term hits do not proceed to WARC extraction or publication-date recovery. All semantic analyses use document publication year, defined as `lsc_year`, because the semantic question concerns the date of the discourse rather than the date on which a crawler observed the page. The semantic context table therefore includes only WARC-validated, English-language, deduplicated contexts with parseable publication dates in the 2014–2026 window.

For ADHD and autism, semantic estimates are frame-aware. The main target strata are clinical-only, lived-only, and mixed discourse, together with a duplicated substantive-core aggregate. Non-substantive or insufficient contexts and substantive-other contexts are retained for composition and quality diagnostics but excluded from the main semantic estimates. Baseline terms are not frame-labelled because the clinical versus lived-experience distinction is specific to ADHD/autism discourse.

## Frame Classification

Frame classification is included before the semantic analyses because target-term contexts may change not only in meaning but also in discourse composition. In web text, ADHD and autism may be framed as diagnoses, disorder constructs, service categories, identities, lived experiences, community labels, or incidental boilerplate. Treating all target contexts as one semantic population would risk conflating lexical semantic change with shifts in the prevalence of these frames. This concern follows Pisl et al. (2025), who show that apparent semantic-severity trends can be explained by the changing mental-health context in which a term appears.

The annotation unit is the target sentence plus adjacent sentence context from the shared LSC context table. Each ADHD/autism passage is labelled hierarchically. First, the passage is coded for whether it contains substantive target discourse. Passages that are thin, list-like, navigational, promotional, generic, incidental, noisy, or otherwise insufficient for target-specific interpretation are assigned to the non-substantive or insufficient category. Substantive passages are then coded on two non-exclusive axes: whether clinical framing is present and whether lived-experience framing is present. Clinical framing covers diagnosis, disorder status, symptoms, impairment, treatment, services, medication, research, epidemiology, DSM/ICD-style categories, and educational or clinical support needs. Lived-experience framing covers identity, self-understanding, family or first-person experience, neurodivergent community, masking, stigma, accommodation, everyday coping, belonging, pride, and embodied or social experience.

The two frame axes are converted deterministically into five derived strata. Substantive passages with clinical but not lived-experience framing are labelled clinical-only; passages with lived-experience but not clinical framing are labelled lived-only; passages with both are labelled mixed; and substantive passages with neither are labelled substantive-other. For non-substantive passages, clinical and lived-experience labels are structurally undefined rather than ordinary negative examples.

Labels were produced through a human-led, LLM-assisted annotation procedure. A 200-passage human pilot was used to refine the codebook. A locked version of the codebook was then applied to 3,000 further passages using schema-validated LLM annotation. A separate critic model ranked likely annotation errors; the highest-priority cases and a random residual audit sample were reviewed by the human coder before labels were finalised. Critic suggestions were advisory only: labels changed only when explicitly reviewed by the human coder. This follows the annotator-critic-human-correction logic proposed for efficient LLM-assisted annotation while keeping final adjudication human-controlled (Lin et al. 2025).

The corrected labels were used to train a hierarchical classifier over all-MPNET-base-v2 passage embeddings. The classifier uses three balanced logistic-regression heads with standardised features: one head predicts substantive target discourse for all labelled examples, while the clinical and lived-experience heads are trained only on substantive examples. Year, URL, and domain metadata are excluded from the classifier features to reduce leakage from temporal or source-specific artefacts. A separate 200-passage human validation set was held out from codebook development, LLM annotation, criticism, correction, and model training. After validation, the classifier was applied to all ADHD/autism contexts in the shared semantic table, producing hard frame labels and frame probabilities for downstream analysis.

## Salience

Salience estimates the prominence of each analysis unit in annual Common Crawl source-year slices. It is not a semantic measure in itself; rather, it asks whether ADHD- and autism-related terms become more or less frequent in sampled web discourse over time. The primary denominator is the number of tokens in minimum-length WET records entering term matching for year $`Y`$. The primary numerator is the number of WARC-validated term hits for analysis unit $`u`$. Candidate and WET-validated rates, raw-form composition, publication-year status, and WARC-over-WET retention are retained as diagnostics.

For analysis unit $`u`$ and Common Crawl source year $`Y`$, salience is defined as

``` math
\begin{equation}
\begin{aligned}
\operatorname{Salience}^{\operatorname{WET}}_{u,Y}
&=
\frac{H^{\operatorname{WET}}_{u,Y}}{T_Y}, \\
\operatorname{Salience}^{\operatorname{WARC}}_{u,Y}
&=
\frac{H^{\operatorname{WARC}}_{u,Y}}{T_Y}, \\
\operatorname{Retention}_{u,Y}
&=
\frac{H^{\operatorname{WARC}}_{u,Y}}{H^{\operatorname{WET}}_{u,Y}}.
\end{aligned}
\end{equation}
```

Here, $`H^{\operatorname{WET}}_{u,Y}`$ is the number of WET-validated hits, $`H^{\operatorname{WARC}}_{u,Y}`$ is the number of WARC-validated hits, and $`T_Y`$ is the annual scanned-token denominator. Reported salience rates are scaled to hits per million WET tokens. Retention is defined only where $`H^{\operatorname{WET}}_{u,Y} > 0`$ and is interpreted as a validation diagnostic rather than as a substantive outcome.

This design follows the relative-frequency logic used by Baes et al. (2024) while adapting it to the WET-first, WARC-second Common Crawl pipeline. The WARC-validated rate is the primary prominence series because it reflects term survival after full-record validation and main-text extraction. WET and candidate rates remain important for checking whether the validation process changes the temporal pattern.

## Sentiment

Sentiment captures whether the local connotational environment of a target term becomes more positive or more negative over time. Following the collocate-based logic of Baes et al. (2024), the measure is computed from words and phrases occurring in a $`\pm 5`$-token window around each target or baseline mention. The present study uses NRC–VAD v2.1 valence scores rather than Warriner norms because NRC–VAD provides broader contemporary English coverage and includes multi-word expressions (Mohammad 2025).

The shared context table supplies the mention offsets and $`\pm 5`$-token windows. For each mention, the focal lexical material itself is removed from the scoring window. The remaining context is tokenised and lemmatised with spaCy `en_core_web_sm`. Punctuation, numerals, and one-character tokens are excluded, but stopwords are retained to keep the collocate index close to the Baes implementation. NRC–VAD entries are normalised with the same lemmatisation procedure. Multi-word expressions are matched greedily before unmatched unigram tokens; if several surface entries collapse to the same lemma phrase, their VAD scores are averaged. The same collocate handoff is reused for intensity so that valence and arousal differ only in the VAD dimension being aggregated.

For analysis unit $`u`$, publication year $`Y`$, and reported stratum $`s`$, annual sentiment is

``` math
\begin{equation}
\operatorname{Sentiment}_{u,Y,s}
=
\frac{
\sum_{w \in C_{u,Y,s}} f_{w,u,Y,s} V(w)
}{
\sum_{w \in C_{u,Y,s}} f_{w,u,Y,s}
},
\end{equation}
```

where $`C_{u,Y,s}`$ is the set of NRC–VAD-matched collocates in the local windows, $`f_{w,u,Y,s}`$ is the frequency of collocate $`w`$, and $`V(w)`$ is its NRC–VAD valence score. Coverage diagnostics record the number of contexts, candidate collocate positions, matched collocate occurrences, unique matched items, and matched-token coverage rate for each unit-year-stratum cell.

## Intensity

Intensity operationalises vertical concept creep as change in the affective arousal of local target contexts. A declining arousal trajectory may be consistent with vertical creep, but it is not interpreted as sufficient evidence on its own because arousal can also reflect advocacy, crisis framing, stigma, newsworthiness, clinical severity, or support-oriented discourse. The measure therefore uses the same local-collocate handoff as Sentiment and differs only in the VAD score being averaged.

Annual intensity for analysis unit $`u`$, publication year $`Y`$, and reported stratum $`s`$ is

``` math
\begin{equation}
\operatorname{Intensity}_{u,Y,s}
=
\frac{
\sum_{w \in C_{u,Y,s}} f_{w,u,Y,s} A(w)
}{
\sum_{w \in C_{u,Y,s}} f_{w,u,Y,s}
},
\end{equation}
```

where $`A(w)`$ is the NRC–VAD arousal score for collocate $`w`$. All preprocessing, target-term exclusion, lemmatisation, multi-word matching, frame stratification, and coverage reporting are inherited from the sentiment handoff. This shared preprocessing contract prevents valence and arousal estimates from diverging because of tokenisation or lexicon-matching choices.

## Breadth

Breadth operationalises horizontal concept creep as contextual dispersion: the more diverse the contexts in which a target term appears, the higher its breadth score. Baes et al. (2024) estimate breadth using sentence-level contextual embeddings. This study replaces a generic sentence-embedding representation with XL-LEXEME, a target-aware word-in-context model designed for lexical semantic change detection (Cassotti et al. 2023). The substitution is methodologically important because ADHD, autism, and the baseline terms are analysed as target uses within local passages rather than as undifferentiated sentence topics.

For ADHD and autism, all markable contexts in the three core substantive frames enter the breadth analysis and are also duplicated into the substantive-core aggregate. Baseline terms remain unframed and are deterministically sampled with a cap of 1,000 contexts per baseline-year, stratified by registered domain to limit domination by high-volume websites. Target contexts are not down-sampled, because the target frame strata are the substantive focus and some frame-year cells are comparatively sparse.

Each candidate context is marked with explicit XL-LEXEME target delimiters. The target sentence is used first; if it is too short or the target cannot be marked reliably, the sentence-plus-adjacent context is used. Contexts that cannot be marked are excluded and saved as diagnostics. Identical marked contexts are encoded once and reused through an embedding index. Annual breadth is computed from L2-normalised target-token embeddings using mean pairwise cosine distance.

For analysis unit $`u`$, publication year $`Y`$, and reported stratum $`s`$, with $`N_{u,Y,s}`$ contextual embeddings $`\mathbf{v}_1,\ldots,\mathbf{v}_{N_{u,Y,s}}`$, breadth is

``` math
\begin{equation}
\operatorname{Breadth}_{u,Y,s}
=
\frac{2}{N_{u,Y,s}(N_{u,Y,s}-1)}
\sum_{i=1}^{N_{u,Y,s}-1}
\sum_{j=i+1}^{N_{u,Y,s}}
\left(
1 -
\frac{\mathbf{v}_i \cdot \mathbf{v}_j}
{\lVert \mathbf{v}_i \rVert \lVert \mathbf{v}_j \rVert}
\right).
\end{equation}
```

Higher values indicate greater average dissimilarity among target uses in that year and stratum. The implementation uses a closed-form mean-pairwise calculation over normalised vectors for the main score, avoiding the need to materialise all pairwise distances except for diagnostics.

## Thematic Evolution

The thematic analysis is designed as an interpretive layer, not as a scalar semantic-change index. Baes et al. (2024) include thematic content as a complement to sentiment, intensity, breadth, and salience, using a top-down pathologisation dictionary in their case study. The present study instead uses a bottom-up topic-modelling approach because ADHD/autism discourse is expected to include diagnosis, services, education, workplace accommodation, parenting, neurodiversity, identity, stigma, advocacy, and everyday experience, not only pathologisation.

The modelling unit is a target-centred passage from the shared semantic context table, with frame labels and raw target forms retained as metadata. Text is cleaned minimally to remove obvious boilerplate residue and near-duplicates while preserving lexical content. Topics are estimated using the BERTopic pipeline: embedding, dimensionality reduction, density-based clustering, c-TF-IDF topic representation, and annual topic-prevalence estimation. The principal outputs are topic labels, representative passages, topic prevalence by year, outlier rates, and frame-aware topic summaries where sample size permits. Topic validity is assessed through manual inspection of top terms, representative passages, temporal stability, domain concentration, and whether topics describe target-relevant discourse rather than generic website genres.

## Uncertainty, Trend Models, and Diagnostics

Annual estimates are the primary objects of interpretation. For sentiment, intensity, and breadth, uncertainty intervals are estimated by document-level bootstrap resampling within each analysis-unit, year, and frame-stratum cell, using 500 bootstrap repetitions and the 2.5th and 97.5th percentiles of the bootstrap distribution. The document is the resampling unit because multiple mentions and collocates from the same document are not independent.

Each scalar measure is summarised with a compact annual trend model based on the reporting strategy of Baes et al. (2024). For measure $`M`$, unit $`u`$, year $`Y`$, and stratum $`s`$, the main descriptive model is

``` math
\begin{equation}
M_{u,Y,s}
=
\alpha_{u,s}
+
\beta_{u,s}(Y - \bar{Y})
+
\varepsilon_{u,Y,s},
\end{equation}
```

where $`\beta_{u,s}`$ is the annual linear slope and $`\bar{Y}`$ is the centre of the observed year range. The trend tables record the slope per year, standard error, $`p`$-value, adjusted $`R^2`$, standardised year coefficient, and a residual autocorrelation diagnostic. Because the annual series span only 13 years and are further stratified by frame for ADHD and autism, these models are treated as descriptive summaries of direction and strength rather than as causal or population-level time-series tests.

Residual autocorrelation is checked with a Durbin–Watson-style diagnostic. When a scalar series is flagged, an AR(1)-transformed sensitivity slope is reported in the corresponding trend table; otherwise the ordinary least-squares estimate remains the main summary. Quadratic fits are retained only as diagnostics for visibly curved trajectories and are not used as the default model.

All downstream analyses carry diagnostic tables alongside the main estimates. These include annual sample size, document and domain counts, raw-form composition, low-volume frame-year flags, VAD coverage, top contributing collocates, WARC retention for salience, and domain-concentration checks where available. Frame-specific target estimates are flagged when they contain fewer than 100 contexts or fewer than 50 documents. These diagnostics do not automatically exclude observations, but they determine how cautiously individual annual points or frame-specific trends should be interpreted.

# Results

# Discussion

# Conclusion

# Appendices

<div id="refs" class="references csl-bib-body hanging-indent">

<div id="ref-alperTikTokAlgorithmicallyMediated2025" class="csl-entry">

Alper, Meryl, Jessica Sage Rauchberg, Ellen Simpson, Josh Guberman, and Sarah Feinberg. 2025. “TikTok as Algorithmically Mediated Biographical Illumination: Autism, Self-Discovery, and Platformed Diagnosis on \#Autisktok.” *New Media & Society* 27 (3): 1378–96. <https://doi.org/10.1177/14614448231193091>.

</div>

<div id="ref-aragon-guevaraReachAccuracyInformation2025" class="csl-entry">

Aragon-Guevara, Diego, Grace Castle, Elisabeth Sheridan, and Giacomo Vivanti. 2025. “The Reach and Accuracy of Information on Autism on TikTok.” *Journal of Autism and Developmental Disorders* 55 (6): 1953–58. <https://doi.org/10.1007/s10803-023-06084-6>.

</div>

<div id="ref-CloudComputingServices" class="csl-entry">

AWS. n.d. “Cloud Computing Services - Amazon Web Services (AWS).” In *Amazon Web Services, Inc.* Https://aws.amazon.com/.

</div>

<div id="ref-baackCriticalAnalysisLargest2024" class="csl-entry">

Baack, Stefan. 2024. “A Critical Analysis of the Largest Source for Generative AI Training Data: Common Crawl.” *Proceedings of the 2024 ACM Conference on Fairness, Accountability, and Transparency* (New York, NY, USA), FAccT ’24, June, 2199–208. <https://doi.org/10.1145/3630106.3659033>.

</div>

<div id="ref-baesSemanticShiftsMental2023" class="csl-entry">

Baes, Naomi, Nick Haslam, and Ekaterina Vylomova. 2023. “Semantic Shifts in Mental Health-Related Concepts.” In *Proceedings of the 4th Workshop on Computational Approaches to Historical Language Change*, edited by Nina Tahmasebi, Syrielle Montariol, Haim Dubossarsky, et al. Association for Computational Linguistics. <https://doi.org/10.18653/v1/2023.lchange-1.13>.

</div>

<div id="ref-baesMultidimensionalFrameworkEvaluating2024" class="csl-entry">

Baes, Naomi, Nick Haslam, and Ekaterina Vylomova. 2024. “A Multidimensional Framework for Evaluating Lexical Semantic Change with Social Science Applications.” In *Proceedings of the 62nd Annual Meeting of the Association for Computational Linguistics (Volume 1: Long Papers)*, edited by Lun-Wei Ku, Andre Martins, and Vivek Srikumar. Association for Computational Linguistics. <https://doi.org/10.18653/v1/2024.acl-long.76>.

</div>

<div id="ref-baesSemanticInflationTrauma2023" class="csl-entry">

Baes, Naomi, Ekaterina Vylomova, Michael Zyphur, and Nick Haslam. 2023. “The Semantic Inflation of ‘Trauma’ in Psychology.” *Psychology of Language and Communication* 27 (1): 23–45. <https://doi.org/10.58734/plc-2023-0002>.

</div>

<div id="ref-bolinskiEffectEmentalHealth2020" class="csl-entry">

Bolinski, F., N. Boumparis, A. Kleiboer, P. Cuijpers, D. D. Ebert, and H. Riper. 2020. “The Effect of e-Mental Health Interventions on Academic Performance in University and College Students: A Meta-Analysis of Randomized Controlled Trials.” *Internet Interventions* 20 (April): 100321. <https://doi.org/10.1016/j.invent.2020.100321>.

</div>

<div id="ref-campbellHistoricalLinguisticsIntroduction2013" class="csl-entry">

Campbell, Lyle. 2013. *Historical Linguistics: An Introduction*. NED - New edition, 3. Edinburgh University Press. <https://www.jstor.org/stable/10.3366/j.ctt1g0b5gq>.

</div>

<div id="ref-cassottiXLLEXEMEWiCPretrained2023" class="csl-entry">

Cassotti, Pierluigi, Lucia Siciliani, Marco DeGemmis, Giovanni Semeraro, and Pierpaolo Basile. 2023. “XL-LEXEME: WiC Pretrained Model for <span class="nocase">Cross-Lingual LEXical sEMantic changE</span>.” In *Proceedings of the 61st Annual Meeting of the Association for Computational Linguistics (Volume 2: Short Papers)*, edited by Anna Rogers, Jordan Boyd-Graber, and Naoaki Okazaki. Association for Computational Linguistics. <https://doi.org/10.18653/v1/2023.acl-short.135>.

</div>

<div id="ref-CommonCrawlGet" class="csl-entry">

Common Crawl Team. 2024. “Get Started.” In *Common Crawl*. Https://commoncrawl.org/get-started.

</div>

<div id="ref-cuiDSM5ChangesCOVID192026" class="csl-entry">

Cui, Zishan, Anshula Ambasta, Wade Thompson, Ken Bassett, Greg Carney, and Colin Dormuth. 2026. “DSM-5 Changes, COVID-19, and ADHD Diagnosis Rates in Individuals Younger Than 30 Years.” *JAMA Network Open* 9 (4): e265775. <https://doi.org/10.1001/jamanetworkopen.2026.5775>.

</div>

<div id="ref-devriesExploringConceptCreep2025" class="csl-entry">

<span class="nocase">de Vries, Wietske, Laura Batstra, and Arjen van Assen</span>. 2025. “Exploring Concept Creep: Youth’s Portrayal of ADHD on TikTok.” *SSM - Mental Health* 8 (December): 100489. <https://doi.org/10.1016/j.ssmmh.2025.100489>.

</div>

<div id="ref-fabianoDiagnosticInflationDSM2020" class="csl-entry">

Fabiano, Fabian, and Nick Haslam. 2020. “Diagnostic Inflation in the DSM: A Meta-Analysis of Changes in the Stringency of Psychiatric Diagnosis from DSM-III to DSM-5.” *Clinical Psychology Review* 80 (August): 101889. <https://doi.org/10.1016/j.cpr.2020.101889>.

</div>

<div id="ref-flearyRelationshipHealthLiteracy2022" class="csl-entry">

Fleary, Sasha A., Patrece L. Joseph, Carolina Gonçalves, Jessica Somogie, and Jessica Angeles. 2022. “The Relationship Between Health Literacy and Mental Health Attitudes and Beliefs.” *HLRP: Health Literacy Research and Practice* 6 (4): e270–79. <https://doi.org/10.3928/24748307-20221018-01>.

</div>

<div id="ref-foulkesAreMentalHealth2023" class="csl-entry">

Foulkes, Lucy, and Jack L. Andrews. 2023. “Are Mental Health Awareness Efforts Contributing to the Rise in Reported Mental Health Problems? A Call to Test the Prevalence Inflation Hypothesis.” *New Ideas in Psychology* 69 (April): 101010. <https://doi.org/10.1016/j.newideapsych.2023.101010>.

</div>

<div id="ref-furediCulturalUnderpinningConcept2016" class="csl-entry">

Furedi, Frank. 2016. “The Cultural Underpinning of Concept Creep.” *Psychological Inquiry* 27 (1): 34–39. <https://doi.org/10.1080/1047840X.2016.1111120>.

</div>

<div id="ref-ginappExperiencesAdultsADHD2023" class="csl-entry">

Ginapp, Callie M., Norman R. Greenberg, Grace Macdonald-Gagnon, Gustavo A. Angarita, Krysten W. Bold, and Marc N. Potenza. 2023. “The Experiences of Adults with ADHD in Interpersonal Relationships and Online Communities: A Qualitative Study.” *SSM - Qualitative Research in Health* 3 (June): 100223. <https://doi.org/10.1016/j.ssmqr.2023.100223>.

</div>

<div id="ref-gjurkovicPANDORATalksPersonality2021" class="csl-entry">

Gjurković, Matej, Vanja Mladen Karan, Iva Vukojević, Mihaela Bošnjak, and Jan Snajder. 2021. “PANDORA Talks: Personality and Demographics on Reddit.” In *Proceedings of the Ninth International Workshop on Natural Language Processing for Social Media*, edited by Lun-Wei Ku and Cheng-Te Li. Association for Computational Linguistics. <https://doi.org/10.18653/v1/2021.socialnlp-1.12>.

</div>

<div id="ref-grimmerTextDataNew2022" class="csl-entry">

Grimmer, Justin, Margaret E. Roberts, and Brandon M. Stewart. 2022. *Text as Data: A New Framework for Machine Learning and the Social Sciences*. Princeton University Press.

</div>

<div id="ref-hamiltonDiachronicWordEmbeddings2016" class="csl-entry">

Hamilton, William L., Jure Leskovec, and Dan Jurafsky. 2016. “Diachronic Word Embeddings Reveal Statistical Laws of Semantic Change.” In *Proceedings of the 54th Annual Meeting of the Association for Computational Linguistics (Volume 1: Long Papers)*, edited by Katrin Erk and Noah A. Smith. Association for Computational Linguistics. <https://doi.org/10.18653/v1/P16-1141>.

</div>

<div id="ref-haslamConceptCreepPsychologys2016" class="csl-entry">

Haslam, Nick. 2016. “Concept Creep: Psychology’s Expanding Concepts of Harm and Pathology.” *Psychological Inquiry* 27 (1): 1–17. <https://doi.org/10.1080/1047840X.2016.1082418>.

</div>

<div id="ref-haslamHarmInflationMaking2020" class="csl-entry">

Haslam, Nick, Brodie C. Dakin, Fabian Fabiano, et al. 2020. “Harm Inflation: Making Sense of Concept Creep.” *European Review of Social Psychology* 31 (1): 254–86. <https://doi.org/10.1080/10463283.2020.1796080>.

</div>

<div id="ref-iacobComputationalAnalysisEmergence2026" class="csl-entry">

Iacob, Alina, and Ana Sabina Uban. 2026. “A Computational Analysis of the Emergence of <span class="nocase">Therapy-speak</span> in Social Media.” In *The Proceedings for the 6th International Workshop on Computational Approaches to Language Change (LChange’26)*, edited by Nina Tahmasebi, Pierluigi Cassotti, Syrielle Montariol, et al. Association for Computational Linguistics. <https://doi.org/10.18653/v1/2026.lchange-1.12>.

</div>

<div id="ref-isern-masUnmaskingTherapyspeak2025" class="csl-entry">

Isern-Mas, Carme, and Manuel Almagro. 2025. *Unmasking Therapy-Speak*. ResearchGate.

</div>

<div id="ref-jm3" class="csl-entry">

Jurafsky, Daniel, and James H. Martin. 2026. *Speech and Language Processing: An Introduction to Natural Language Processing, Computational Linguistics, and Speech Recognition with Language Models*. 3rd ed.

</div>

<div id="ref-kangConvergingRepresentationsAttentionDeficit2025" class="csl-entry">

Kang, Jemima, Nick Haslam, and Mike Conway. 2025. “Converging Representations of Attention-Deficit/Hyperactivity Disorder and Autism on Social Media: Linguistic and Topic Analysis of Trends in Reddit Data.” *Journal of Medical Internet Research* 27 (1): e70914. <https://doi.org/10.2196/70914>.

</div>

<div id="ref-linACTHumanMultimodal2025" class="csl-entry">

Lin, Lequan, Dai Shi, Andi Han, et al. 2025. “ACT as Human: Multimodal Large Language Model Data Annotation with Critical Thinking.” In *arXiv.org*. Https://arxiv.org/abs/2511.09833v2.

</div>

<div id="ref-manningHumanLanguageUnderstanding2022" class="csl-entry">

Manning, Christopher D. 2022. “Human Language Understanding & Reasoning.” *Daedalus* 151 (2): 127–38. <https://doi.org/10.1162/daed_a_01905>.

</div>

<div id="ref-martinChangingPrevalenceADHD2025" class="csl-entry">

Martin, Alex F., G. James Rubin, M. Brooke Rogers, et al. 2025. “The Changing Prevalence of ADHD? A Systematic Review.” *Journal of Affective Disorders* 388 (November): 119427. <https://doi.org/10.1016/j.jad.2025.119427>.

</div>

<div id="ref-mckechnieAttentiondeficitHyperactivityDisorder2023" class="csl-entry">

McKechnie, Douglas G. J., Elizabeth O’Nions, Sandra Dunsmuir, and Irene Petersen. 2023. “Attention-Deficit Hyperactivity Disorder Diagnoses and Prescriptions in UK Primary Care, 2000–2018: Population-Based Cohort Study.” *BJPsych Open* 9 (4): e121. <https://doi.org/10.1192/bjo.2023.512>.

</div>

<div id="ref-page18HowHarnessPower" class="csl-entry">

Medaris, Anna. 2024. “How to Harness the Power of Therapy-Speak.” *Https://Www.apa.org* 55 (6).

</div>

<div id="ref-mohammadNRCVADLexicon2025" class="csl-entry">

Mohammad, Saif M. 2025. “NRC VAD Lexicon V2: Norms for Valence, Arousal, and Dominance for over 55k English Terms.” In *arXiv.org*. Https://arxiv.org/abs/2503.23547v1.

</div>

<div id="ref-ndourRomanticisationMentalHealth2025" class="csl-entry">

Ndour, Awa, and Lucy Foulkes. 2025. “The Romanticisation of Mental Health Problems in Adolescents and Its Implications: A Narrative Review.” *European Child & Adolescent Psychiatry* 34 (8): 2297–326. <https://doi.org/10.1007/s00787-025-02701-0>.

</div>

<div id="ref-pislRevisitingSemanticSeverity2025" class="csl-entry">

Pisl, Vojtech, Ana-Maria Bucur, and Ioana R. Podina. 2025. “Revisiting the Semantic Severity of Anxiety and Depression: Computational Linguistic Study of Normalization and Pathologization.” *Journal of Medical Internet Research* 27 (1): e73950. <https://doi.org/10.2196/73950>.

</div>

<div id="ref-sampognaImpactSocialMarketing2017" class="csl-entry">

Sampogna, G., I. Bakolis, S. Evans-Lacko, E. Robinson, G. Thornicroft, and C. Henderson. 2017. “The Impact of Social Marketing Campaigns on Reducing Mental Health Stigma: Results from the 2009–2014 Time to Change Programme.” *European Psychiatry* 40 (February): 116–22. <https://doi.org/10.1016/j.eurpsy.2016.08.008>.

</div>

<div id="ref-schomerusEvolutionPublicAttitudes2012" class="csl-entry">

Schomerus, G., C. Schwahn, A. Holzinger, et al. 2012. “Evolution of Public Attitudes about Mental Illness: A Systematic Review and Meta-Analysis.” *Acta Psychiatrica Scandinavica* 125 (6): 440–52. <https://doi.org/10.1111/j.1600-0447.2012.01826.x>.

</div>

<div id="ref-spencerIsntEveryoneLittle2021" class="csl-entry">

Spencer, Lucienne, and Havi Carel. 2021. “‘Isn’t Everyone a Little OCD?’: The Epistemic Harms of Wrongful Depathologization.” *Philosophy of Medicine* 2 (1). <https://doi.org/10.5195/pom.2021.19>.

</div>

<div id="ref-vylomovaSemanticChangesHarmrelated2021" class="csl-entry">

Vylomova, Ekaterina, and Nick Haslam. 2021. “Semantic Changes in Harm-Related Concepts in English.” In *Computational Approaches to Semantic Change*. Language Science Press. <https://doi.org/10.5281/zenodo.5040304>.

</div>

<div id="ref-wenzekCCNetExtractingHigh2019" class="csl-entry">

Wenzek, Guillaume, Marie-Anne Lachaux, Alexis Conneau, et al. 2019. *CCNet: Extracting High Quality Monolingual Datasets from Web Crawl Data*. arXiv:1911.00359. arXiv. <https://doi.org/10.48550/arXiv.1911.00359>.

</div>

<div id="ref-xiaoHaveConceptsAnxiety2023" class="csl-entry">

Xiao, Yu, Naomi Baes, Ekaterina Vylomova, and Nick Haslam. 2023. “Have the Concepts of ‘Anxiety’ and ‘Depression’ Been Normalized or Pathologized? A Corpus Study of Historical Semantic Change.” *PLOS ONE* 18 (6): e0288027. <https://doi.org/10.1371/journal.pone.0288027>.

</div>

<div id="ref-zeidanGlobalPrevalenceAutism2022" class="csl-entry">

Zeidan, Jinan, Eric Fombonne, Julie Scorah, et al. 2022. “Global Prevalence of Autism: A Systematic Review Update.” *Autism Research* 15 (5): 778–90. <https://doi.org/10.1002/aur.2696>.

</div>

</div>
