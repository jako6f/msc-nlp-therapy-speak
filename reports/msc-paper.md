# Writing

Status: Writing

#### Archive

[Version 1.0](https://app.notion.com/p/Version-1-0-372431d9647280c4ac51f1995ca88061?pvs=21)

[Version 2.0](https://app.notion.com/p/Version-2-0-372431d9647280988554e0a611499273?pvs=21)

#### Current Version

# Introduction ✅

It is widely recognised that many countries are experiencing an unfolding *mental health crisis*. Evidence of escalating mental health complaints comes from surveys, epidemiological studies, and rising prescriptions of psychiatric medications (Haidt, 2025). Particularly substantial increases in prevalence have been reported for the neurodevelopmental disorders autism (Zeidan et al. 2022) and attention-deficit hyperactivity disorder (ADHD) (Cui et al. 2026; McKechnie et al. 2023). In Australia, for example, the number of people with autism increased by 42% from 2018 to 2022, including a 95% increase among women and girls (ABS 2023). In the United Kingdom, ADHD diagnoses increased by factors of two, three, and more than 10 for boys, girls, and adults, respectively, from 2000 to 2018 (McKechnie et al. 2023).

Concurrently, mental health awareness campaigns have proliferated (Bolinski et al. 2020), and mental health-related discourse has become increasingly common in everyday life (Foulkes and Andrews 2023; Medaris 2024). Again, ADHD and autism are particularly stark examples. In June 2026, ADHD and autism had been hashtagged in more than 5.4 million and 4 million videos on TikTok, respectively. An analysis of a LexisNexis dataset revealed that from January to May 2024, ADHD was the subject of 25,080 media articles, compared with 5775 articles during the equivalent period in 2014 (Martin et al. 2025). 

On the one hand, this popularisation of mental health related discourse in everyday life has lead to undeniably positive outcomes. It may give people language for subtle emotional states (Medaris 2024), improve mental health literacy (Schomerus et al. 2012), and thereby reduce stigma around mental illness (Sampogna et al. 2017; Fleary et al. 2022). It may also support better recognition and more accurate reporting of mental health problems (Foulkes and Andrews 2023), erode barriers to help-seeking that contribute to the ongoing under-treatment of some conditions (Haslam, 2026), and facilitate online identity and community formation, particularly around ADHD and autism (Ginapp et al. 2023). 

On the other hand, the concurrence of rising prevalence rates of mental ill health and the proliferation of mental health-related discourse in everyday life has prompted scholars to interrogate the relationship between the two. Foulkes and Andrews (2023) term this account the *prevalence inflation hypothesis*, which posits a cyclical, mutually reinforcing dynamic. On one side of the cycle, rising prevalence may promote the permeation of mental health terminology into mainstream discourse. On the other, the increased availability of such discourse may itself inflate prevalence by encouraging overdiagnosis, overinterpretation, or pathologisation, whereby milder or more transient forms of distress are understood and reported as clinical mental health problems, often in conjunction with self-diagnosis (Foulkes and Andrews 2023; Beeker et al. 2021; Haslam 2026).

The mechanism is not merely linguistic. Once individuals interpret and label their psychological experiences as symptoms of a mental health condition, this may alter self-concept and behaviour (Foulkes and Andrews 2023; Alper et al. 2025). In stronger cases, such labelling may become self-fulfilling: ordinary distress, reframed as symptomatic of disorder, may lead to patterns of attention, avoidance, identification, or help-seeking that intensify the very symptoms being labelled (Foulkes and Andrews 2023). This process may be further amplified where mental health problems are glamorised or romanticised, that is, represented as socially desirable, identity-conferring, aesthetically meaningful, or markers of depth and authenticity. Under these conditions, diagnostic and quasi-diagnostic labels may acquire social value, making self-labelling attractive rather than merely explanatory (Ndour and Foulkes 2025).

These concerns are particularly relevant to ADHD and autism, where clinicians have reported increases in self-diagnosed presentations (Hartnett and Cummings 2024; Weigle and Shafi 2024). They may also be especially pronounced among adolescents, given their heightened susceptibility to rumination, peer influence, identity formation, social reward, and media messaging (Foulkes and Andrews 2023; Ndour and Foulkes 2025).

Another corollary of the diffusion of mental health language into popular culture is a phenomenon described as *therapy-speak*, “the imprecise and superficial integration of psychotherapy language into everyday communication” (Isern-Mas and Almagro 2025). The consequences are twofold. Firstly, therapy-speak may spread inaccurate psychological information. An analysis of the 100 most popular TikTok videos about ADHD found that more than half (55%) of the characteristics attributed to ADHD by video creators did not align with DSM-5 diagnostic criteria (de Vries et al., 2025). Similarly, an analysis of the 133 most-viewed TikTok videos tagged #autism found that only 27% were rated as accurate, 41% as inaccurate, and 32% as containing potentially misleading overgeneralisations (Aragon-Guevara et al., 2023). Secondly, therapy-speak may erode the meaning and relevance of mental-health-related terms, a process that can also be understood as trivialisation. This can contribute to hermeneutical injustice by depriving people who actually live with a certain mental condition of the words they need to describe their experiences (Isern-Mas and Almagro 2025) and by reducing their symptoms to mere personality traits, thereby denying them a fully recognised psychiatric identity (Spencer and Carel 2021).

This latter concern—the erosion of meaning in psychotherapy terms—is the focus of the present study, which investigates diachronic lexical semantic change in the terms ADHD and autism in general web discourse. The following literature review first synthesises empirical evidence on lexical semantic change (LSC) in mental-health-related terms over recent decades, before reviewing the computational approaches used to study these processes and identifying the gap addressed by this project.

# Related Work ✅

## Lexical semantic change and concept creep theory

Lexical semantic change (LSC) refers to shifts in a word’s meaning while its grammatical function remains stable, and constitutes a common form of language change (Campbell 2013). For example, cloud, initially a meteorological term, broadened in usage to refer to internet-based data storage.

*Concept creep theory* posits that harm-related concepts are particularly prone to LSC, specifically gradual semantic broadening. As such, many harm-related terms, including addiction, bullying, harassment, prejudice, and trauma, are reported to have expanded in meaning since at least the 1970s (Haslam 2016; Baes, Vylomova, et al. 2023). Concept creep distinguishes between two forms of LSC that can occur concurrently: vertical creep and horizontal creep. Vertical creep refers to the loosening of definitions to include milder instances (semantic severity or intensity), whereas horizontal creep refers to the extension of definitions to encompass qualitatively new phenomena (semantic breadth). Concepts of mental illness are considered harm-related because distress and dysfunction are fundamental to their definition (Wakefield 1992). Previous studies have, therefore, examined concept creep across different mental-health-related concepts. 

Vylomova and Haslam (2021) found that "*trauma*" and "*addiction*" (together with three non-mental health-related concepts: “bullying,” “prejudice,” and “harassment”) exhibited both vertical and horizontal creep in a corpus of approximately 800,000 psychology article abstracts from 875 journals dating back to the 1960s. Similar, though weaker, patterns were observed in a general-language corpus combining the Corpus of Contemporary American English (CoCA) and the Corpus of Historical American English (CoHA) from the 1970s to the 2010s. Subsequent studies using the same corpora and time periods have reported comparable findings for other mental-health-related concepts. Baes, Vylomova, et al. (2023) found evidence of vertical creep in "*trauma*" in the psychology corpus. Baes, Haslam, et al. (2023) reported vertical creep in "*addiction*", "*anger*", "*stress*", and "*worry*" in the psychology corpus, and in "*addiction*", "*grief*", "*stress*", and "*worry*" in the general corpus. Baes et al. (2024) further found that the broader concepts "*mental health*" and "*mental illness*" both expanded horizontally in the psychology corpus.

Not all findings align with this pattern. Xiao et al. (2023) reported that the semantic severity of "*anxiety*" and "*depression*" increased in both Vylomova and Haslam’s psychology corpus and their general-language corpus, contrary to concept-creep expectations. This unexpected result may reflect measurement unspecificity, particularly the absence of discourse-context and construct distinctions. For example, "depression" may refer to psychiatric depression, but also to meteorological or economic phenomena. Similarly, "*anxiety*" may refer to a nosological category, a disorder construct, or an underlying human experience. In a replication study, Pisl et al. (2025) showed that this apparent increase in semantic severity could be attributed to changing discourse composition, specifically shifts in the balance between clinical or nosological contexts and lived-experience contexts, rather than to intrinsic semantic change alone. In their analysis of lead paragraphs from New York Times articles published between 1970 and 2023, the time effect for “depression” became nonsignificant after controlling for mental-health context.

Iacob and Uban (2026) examined a range of terms commonly associated with therapy-speak, including “toxic”, “bipolar”, “psychopath”, “narcissistic”, and “triggered”, and reported mixed results. In an extension of Vylomova and Haslam’s psychology corpus, most terms exhibited horizontal creep. By contrast, in two Reddit corpora comprising comments from psychology-oriented subreddits and general subreddits between 2010 and 2025, most terms showed a narrowing in breadth. Overall, the authors found that long-established psychological terms such as “OCD”, “bipolar”, and “trauma” displayed little semantic change, whereas terms such as “gaslighting” and “imposter” shifted substantially from year to year.

On balance, many mental-health-related concepts appear to have undergone concept creep, becoming milder and broader over recent decades. Haslam (2020; 2026) theorises several drivers of this process. One is the influence of “opprobrium entrepreneurs”, who seek to cast previously accepted conditions in a more problematic light; by broadening a harm concept, the disapproval associated with its original meaning can come to apply to less severe cases. A second driver is “prevalence-induced conceptual change”, whereby the standards for recognising harm tend to loosen as instances of harm become less common. Finally, Haslam (2020; 2026) points to a broader cultural increase in concern with harm. As harm concepts expand, a wider range of experiences is treated as morally significant and worthy of care, and a greater number of actions come to be seen as harmful.

Whether the broadening of mental-health constructs reflects changes in official diagnostic criteria is unclear. Fabiano and Haslam (2020) showed that no revision of the *Diagnostic and Statistical Manual of Mental Disorders* (DSM) from the third edition onward was reliably more inflationary or deflationary overall. However, specific disorders changed significantly. Most notably, ADHD inflated by 18%, 33%, and then 17% across the three revisions following DSM-III. Autism also inflated by 50% from DSM-III to DSM-III-R, albeit based on a single study, but deflated by 15% from DSM-IV to DSM-5.

## Computational Approaches to Studying Diachronic Lexical Semantic Change

Semantic change has long been studied across linguistics and the social sciences, but lexical semantic change remains difficult to characterise because changes in word meaning are often gradual and less visible than other forms of linguistic change, such as those produced by spelling or grammar reforms (Camboim de Sa et al., 2026). Earlier research largely depended on manual methods, with linguists using historical texts, dictionaries, and corpora to reconstruct how word meanings changed over time. More recently, computational linguistics and natural language processing (NLP) have made it possible to study semantic change at much larger scales (Camboim de Sa et al., 2026). Within NLP, meaning is typically understood in distributional terms: a word’s meaning is inferred from the contexts in which it appears. These contextual patterns can be represented through several computational approaches, including frequency-based measures, topic models, semantic graphs, and embedding-based methods (Camboim de Sa et al., 2026).

Although considerable progress has been made in identifying LSC using these techniques (see Tahmasebi et al., 2018; Kutuzov et al., 2018; Tang, 2018), less attention has been paid to characterising the nature of such changes. To address this gap, Baes et al. (2024) proposed the SIBling framework—Sentiment, Intensity, and Breadth—as a unified, multidimensional approach to characterising LSC. The framework situates concept creep within a broader account of semantic change and links this conceptual model to computational methods, including frequency-based, dictionary-based, and embedding-based techniques. It distinguishes three dimensions along which terms may change over time: vertical drift, horizontal drift, and sentiment. Vertical drift refers to changes in severity or intensity. A term may strengthen, as in “hilarious” shifting from cheerful or amusing to extremely funny, or weaken, as in “trauma” shifting from brain injuries to milder events such as business loss. This dimension maps onto vertical concept creep. Horizontal drift refers to changes in breadth. A term may narrow, as in “doctor” shifting from scholar or teacher to primarily denoting a medical professional, or broaden, as in “cloud” shifting from a meteorological term to internet-based data storage. This dimension maps onto horizontal concept creep. Sentiment refers to changes in connotation. A term may acquire a more positive connotation, as in “geek” shifting from a derogatory term for odd people to someone passionate about a field, or a more negative connotation, as in “retarded” shifting from a neutral term for intellectual disability to a highly pejorative slur. This dimension roughly corresponds to destigmatisation and stigmatisation, which are not directly captured by concept creep theory. According to SIBling, these dimensions can be complemented by examining shifts in target-word frequency, or salience, and in the thematic content of target-word collocates. Despite its conceptual value, however, the framework’s operationalisations should be treated as first implementations rather than settled measures: they have not yet been extensively validated externally, and their measurement choices remain open to refinement.

# The Present Study ✅

Two gaps motivate the present study. The first concerns the evidentiary basis of existing work. Previous research on lexical semantic change in mental-health-related concepts has relied heavily on curated corpora, and often on the same few datasets. Studies of general language have frequently used Vylomova and Haslam’s (2021) combined CoCA and CoHA corpus (e.g., Baes, Haslam, et al., 2023, 2024; Xiao et al., 2023), while studies of psychology-specific language have repeatedly drawn on Vylomova and Haslam’s (2021) psychology corpus (e.g., Baes et al., 2024, 2023; Xiao et al., 2023; Iacob and Uban, 2026). Other work has used Reddit data (Kang et al., 2025; Iacob and Uban, 2026).

These corpus choices matter because the broader aim of this literature is to infer cultural dynamics from lexical change. Curated corpora may partly reflect shifts in editorial policy, audience composition, disciplinary convention, or ideological stance rather than broader changes in public discourse (Pisl et al., 2025). Reddit is also an imperfect proxy for general discourse, as its user base, like that of many social media platforms, is demographically skewed (Gjurković et al., 2021). To date, lexical semantic change in mental-health-related concepts has not been examined systematically in general web discourse.

The second gap concerns the target concepts themselves. Direct evidence on lexical semantic change in ADHD and autism remains sparse. Kang et al. (2025) showed that ADHD and autism appear to have converged semantically on Reddit: from 2019 onward, their contextual similarity increased, and the two terms became more similar to each other than to comparison conditions. However, this finding primarily identifies convergence between the two terms on a single platform. It does not characterise how each concept’s semantic profile has changed over time in general web discourse.

To address these gaps, the present study asks three research questions: (i) how has the salience, or prevalence, of ADHD and autism in general web discourse changed from 2014 to 2026, relative to non-clinical negative emotion baseline terms; (ii) how have the intensity, breadth, and sentiment of ADHD and autism contexts changed over this period, again relative to these baseline terms; and (iii) how has the thematic content of ADHD and autism discourse evolved over time?

Methodologically, the present study adapts Baes et al.’s (2024) SIBling framework, introducing targeted modifications such as target-aware embeddings, an expanded affective lexicon, and frame-aware analysis. It constructs a diachronic corpus of general web discourse using Common Crawl, a large-scale, openly accessible repository of web crawl data. The study spans 2014–2026, from the earliest period with sufficiently consistent and usable data to the most recent available year. An economical and reproducible pipeline extracts quality-filtered web documents containing ADHD, autism, and baseline emotion terms, supporting both frequency (salience) estimation and downstream semantic analysis while ensuring comparability across targets and baselines.

The project contributes by extending concept creep and therapy-speak research to ADHD and autism; shifting the evidentiary base from curated or platform-specific corpora toward general web discourse; implementing a reproducible Common Crawl pipeline for diachronic lexical semantic change research; and evaluating whether SIBling-style measures can be strengthened through target-aware, affective-lexicon-based, and frame-aware methodological adaptations.

# Data and Materials ✅

## Common Crawl

Common Crawl is the largest freely available public archive of web crawl data, totalling more than 10 petabytes across crawls published approximately monthly, each typically containing more than two billion web pages (Baack, 2024, Common Crawl, 2024). Widely used as a web-scale source of language data for corpus construction, NLP research, and large language model pretraining (Baack, 2024; Wenzek et al., 2019), it has been cited in over 12,000 research papers (Common Crawl, 2024). For the present study, its central value lies in breadth: rather than drawing on a single platform, newspaper archive, or curated corpus, Common Crawl provides repeated cross-sections of general web discourse. The archive stores raw HTML and, importantly, collects only a sample of pages from each domain it visits — meaning, for instance, that only a subset of Wikipedia articles will appear, not the full site. Domain selection and page depth are governed by *harmonic centrality*, a graph-based scoring method adopted in 2017 whereby a domain's importance is determined by the volume of direct and indirect inbound links from other domains, with direct links weighted most heavily; higher-scoring domains are both more likely to be crawled and to have more of their pages fetched (Baack, 2024). Despite its enormous size, Common Crawl is neither a complete copy of the web nor a representative sample of it. A growing number of rights holders — including major outlets such as the New York Times — now block Common Crawl via the `robots.txt` standard, largely in response to AI training data concerns, and large social media platforms such as Facebook have done so for considerably longer (Baack, 2024). The data used in this study should therefore not be treated as a representative survey of the web or of public opinion; they reflect what Common Crawl crawled, retained, and made available within these structural constraints.

To collect the data for this study, we built a Common Crawl collection pipeline designed to extract quality-gated general discourse for the analysis of diachronic lexical semantic change in specific target terms, here ADHD and autism, against matched baseline terms. The pipeline workflow is illustrated in Figure 3.1. Target and baseline terms are processed together so that yearly denominators, sampling logic, and quality filters remain comparable across term groups. The pipeline uses Common Crawl’s two main file formats sequentially. WET files provide compact extracted plaintext and are therefore used for large-scale term scanning and yearly prevalence denominators, but they lack the HTML structure and metadata needed for stronger boilerplate filtering. Candidate documents are therefore resolved to their corresponding WARC records, which preserve the full HTML code. Although WARC processing is slower, it enables main-text extraction, boilerplate removal, and metadata recovery. This WET-first, WARC-second design keeps the pipeline economical by reserving expensive WARC processing for candidate documents only.^1

The design consists of two linked tracks. The trend track uses fixed-effort annual samples to estimate how frequently target and baseline terms appear over time. The corpus track builds a larger, quality-gated document corpus for downstream NLP analysis. To reduce the risk that a small number of large websites dominate the corpus, domain caps are applied at 50 WET-validated hit rows per registered domain per Common Crawl crawl. Intermediate summaries and manifests are retained so that each year, crawl, track, and batch remains auditable. The pipeline is designed to run end-to-end on AWS EC2, using S3 as the durable storage and transfer layer for intermediate and final collection artefacts. Yearly crawl selection is deterministic: one Common Crawl snapshot is selected per year near a fixed annual anchor date and then frozen in a crawl map.

Several software choices are methodologically consequential because they affect corpus membership. WET and WARC records are parsed with `warcio`, WARC pointers are resolved through a local `pywb`-based index server, archived HTML is converted to main text with Trafilatura and Resiliparse, and post-extraction filtering uses DataTrove quality filters followed by English-language filtering with `py3langid` (Bevendorff et al., 2018; Barbaresi et al., 2021; Penedo et al., 2024; Lui & Baldwin, 2012)^2

The data collection spans 13 annual Common Crawl snapshots from 2014 to 2026. This window maximises temporal depth while remaining compatible with a stable WET-first, WARC-second workflow. By 2014, Common Crawl provided sufficiently large WET/WARC-format crawls for efficient plaintext scanning, denominator construction, and HTML-based validation; earlier data are less comparable and require additional handling due to older archive formats. The 2026 endpoint reflects the latest collection year available for the project. In the trend track, the pipeline scanned 55.6 million web pages and retained 156,189 pages featuring validated term hits. In the corpus track, it scanned 220 million web pages and retained 336,178 analysis-ready documents. Of these, 87,173 documents contain ADHD or autism target terms. Target membership is non-exclusive: 31,354 documents contain ADHD terms, 67,614 contain autism terms, and 11,795 contain both. The collection was run on an AWS `m7i-flex.large` instance, featuring 2 vCPUs, 8 GiB of RAM, and up to 12.5 Gbps network bandwidth (AWS, n.d.). On this instance type, corpus throughput was approximately one hour per million scanned WET records.

---

---

^1

For a detailed record of the design, configuration, and methodological decisions behind the Common Crawl collection pipeline refer to https://github.com/jako6f/msc-nlp-therapy-speak/blob/main/reports/commoncrawl_corpus_design_and_provenance.md

^2

Both `warcio` and `pywb` are open-source Webrecorder projects; see [https://github.com/webrecorder/warcio](https://github.com/webrecorder/warcio) and [https://github.com/webrecorder/pywb](https://github.com/webrecorder/pywb).

---

## Target Terms

Two target concepts were selected for diachronic lexical semantic change analysis: *ADHD* and *autism*. Target documents were retrieved using the matching expressions shown in Table 3.1. The abbreviation `ASD` (autism spectrum disorder) was retained only when *autism* occurred within $`\pm`$200 characters, reducing false positives from unrelated acronym use. The acronym `ADD` (attention deficit disorder) was not included as a matching expression because ADHD is the current canonical diagnostic label, whereas ADD is older and colloquial (American Psychiatric Association, 2013); more importantly, `ADD` would create substantial false positives in WET scanning because it overlaps with the common verb *add* and with web chrome such as “add to cart”. The broader expression `attention[-\s]?deficit` retains coverage of relevant expanded forms while avoiding this high-noise acronym.

| Concept | Matching expressions |
| --- | --- |
| ADHD | `\badhd\b; attention[-\s]?deficit` |
| Autism | `\bautism\b; \bautistic\b; autism[-\s]?spectrum; \bASD\b` |

*Table header.* Target-term matching expressions.

For comparison, three negative, non-clinical emotion terms were selected with sufficient coverage and interpretable usage: “frustration*”*, “sadness*”*, and “loneliness*”*. These terms are not exact semantic controls for ADHD and autism; they provide a baseline for separating target-specific change from broader shifts in negative affective language in the corpus.

## Preprocessing

Following Baes et al. (2024), preprocessing was organised around analysis-specific corpus representations rather than a single all-purpose text file. The downstream semantic analyses use a shared mention-level context table built from the extraction pipeline’s corpus product. Target and baseline mentions were re-detected using the frozen collection patterns (see Table 3.1), including the ASD disambiguation rule, and overlapping matches within the same analysis unit were resolved by keeping the longest span. This prevents expressions such as autism spectrum from being counted twice as both a phrase and a shorter nested form. Documents containing both ADHD and autism terms were allowed to contribute to both target groups, with separate mention-level contexts emitted for each concept. Same-sentence acronym-and-expansion pairs occurring within a short local window were also collapsed, so cases such as “attention deficit hyperactivity disorder (ADHD)” contribute one conceptual ADHD context rather than two near-duplicate rows. To limit domination by repetitive documents while preserving repeated-use signal, each document could contribute at most three mentions per analysis unit. The semantic time variable was then defined as publication year and only contexts with parseable publication dates between 2014 and 2026 were retained for the shared LSC table. This yielded 293,670 mention contexts from 212,651 documents and 135,771 registered domains, including 28,611 ADHD contexts, 68,253 autism contexts, and separate baseline series for frustration, sadness, and loneliness. For each retained mention, the table stores the matched form, raw-form collapse diagnostics, registered domain, publication and crawl metadata, a ±5-token window for affective collocate analyses, and target-sentence passages for breadth and thematic analyses.

 

---

## NRC–VAD Lexicon

The affective analyses use the NRC Valence, Arousal, and Dominance (VAD) Lexicon v2.1 (Mohammad, 2025), rather than the Warriner norms used in SIBling (Baes et al., 2024). NRC–VAD offers several advantages for the present study. It is substantially larger, providing human ratings for approximately 45,000 English words and 10,000 multi-word phrases, compared with 13,915 words and no phrase entries in Warriner norms. It has also been reported to have higher reliability, with an aggregate reliability estimate of 0.923 across the three VAD dimensions, compared with 0.823 for the Warriner norms.

NRC–VAD scores range from -1 to 1. Higher valence scores indicate more positive affect, higher arousal scores indicate greater emotional activation, and higher dominance scores indicate greater perceived control or power. This study uses valence to estimate whether the local contexts of target terms become more positive or negative over time, and arousal to estimate changes in emotional intensity. Dominance is included in the source lexicon but is not used in the present analyses. The ratings were produced using Best–Worst Scaling, in which annotators are shown four items and asked to identify the item that best and worst represents the property being rated (Mohammad, 2025).

# Methods

## Overview

The analysis adapts the SIBling framework of Baes et al. (2024), which characterises lexical semantic change along three, interpretable dimensions rather than treating change as a single aggregate distance. The study estimates five annual trajectories for ADHD and autism discourse: salience, intensity, breadth, sentiment, and neighbour similarity evolution. Salience measures how often target and baseline terms occur in sampled Common Crawl slices. Intensity and sentiment measure the affective arousal and valence of local collocates, respectively. Breadth measures contextual dispersion among target-aware embeddings. Neighbour similarity evolution tracks how the semantic proximity between each target concept and its recurrent neighbouring terms changes across the study period. All semantic analyses use document publication year as annual time axis. For ADHD and autism, semantic estimates differentiate between clinical/disorder versus lived-experience framing. Baseline terms are not frame-labelled because the same classes don’t apply to them.^4

---

For full code implementation of all analyses refer to [https://github.com/jako6f/msc-nlp-therapy-speak/tree/main/notebooks](https://github.com/jako6f/msc-nlp-therapy-speak/tree/main/notebooks)

## Salience

Salience estimates whether ADHD and autism terms become more or less frequent in sampled Common Crawl web discourse over time. Unlike all semantic analyses which use document publication year, salience is measured on the Common Crawl source-year axis. This choice is technical rather than conceptual: the denominator must cover the full yearly WET sample entering term matching, including pages without target hits, and publication dates are only recovered downstream for WARC-extracted hit documents. A publication-year denominator would therefore be unavailable for the non-hit background corpus. Source year is not identical to publication year (Baack, 2024), so salience should be interpreted as source-year prominence in the sampled crawl rather than as a direct estimate of publication-year prevalence.

For each analysis unit $u$ and Common Crawl source year $Y$, the numerator is the number of WARC-validated term hits, and the denominator is the number of tokens in minimum-length WET records entering the annual scan. Reported salience rates are scaled to hits per million WET tokens:

```latex
\text{Salience}_{u,Y}
=
1{,}000{,}000
\times
\frac{H^{\text{WARC}}_{u,Y}}{T^{\text{WET}}_Y}
```

## Frame Classification

Frame classification is included before the semantic analyses because target-term contexts may change not only in meaning but also in discourse composition. In web text, ADHD and autism may be framed as diagnoses, disorder constructs, service categories, identities, lived experiences, community labels, or incidental boilerplate. Treating all target contexts as one semantic population would risk conflating lexical semantic change with shifts in the prevalence of these frames. This concern follows Pisl et al. (2025), who show that apparent semantic-severity trends can be explained by the changing mental-health context in which a term appears.

The annotation unit is the target sentence plus adjacent sentence context. Each ADHD/autism passage is labelled hierarchically. First, the passage is coded for whether it contains substantive target discourse. Passages that are thin, list-like, navigational, promotional, generic, incidental, noisy, or otherwise insufficient for target-specific interpretation are assigned to the non-substantive or insufficient category. Substantive passages are then coded on two non-exclusive axes: whether clinical framing is present and whether lived-experience framing is present. Clinical framing covers diagnosis, disorder status, symptoms, impairment, treatment, services, medication, research, epidemiology, DSM/ICD-style categories, and educational or clinical support needs. Lived-experience framing covers identity, self-understanding, family or first-person experience, neurodivergent community, masking, stigma, accommodation, everyday coping, belonging, pride, and embodied or social experience. The two frame axes are converted deterministically into five derived strata. Substantive passages with clinical but not lived-experience framing are labelled `clinical-only`; passages with lived-experience but not clinical framing are labelled `lived-only`; passages with both are labelled `mixed`; and substantive passages with neither are labelled `substantive-other`. ^3

Frame labels were generated using an Annotation with Critical Thinking (ACT) workflow, adapted from Lin et al. (2025). Rather than treating LLM labels as final annotations, ACT uses one model as an annotator, a second model as a criticiser that estimates which annotations are most likely to be erroneous, and human adjudication to resolve uncertain or high-risk cases. First, a 200-passage human pilot was used to refine the codebook. The locked codebook was then applied to 3,000 ADHD/autism passages using OpenAI’s Codex GPT-5.5 with high reasoning effort, producing initial machine annotations for the full annotation batch. Gemini 3 Flash Preview, accessed via the Gemini CLI, then reviewed each Codex-generated annotation and assigned an error-risk score. Following ACT, human review was concentrated on the highest-risk cases using a threshold-based selection rule, rather than distributed randomly across the full batch. Human correction remained authoritative throughout: critic outputs were used only to prioritise review, and labels were changed only after manual inspection. This procedure preserved human control over difficult boundary cases while reducing the amount of fully manual annotation required. To guard against critic blind spots, a random sample of lower-risk cases was also inspected.

The corrected labels were used to train a hierarchical classifier over `all-MPNET-base-v2` passage embeddings. The classifier uses three balanced logistic-regression heads with standardised features: one head predicts substantive target discourse for all labelled examples, while the clinical and lived-experience heads are trained only on substantive examples. Year, URL, and domain metadata are excluded from the classifier features to reduce leakage from temporal or source-specific artefacts. A separate 200-passage human validation set was held out from codebook development, LLM annotation, criticism, correction, and model training. After validation, the classifier was applied to all ADHD/autism contexts, producing hard frame labels and frame probabilities for downstream analysis.

---

^5 For full codebook refer to [https://github.com/jako6f/msc-nlp-therapy-speak/tree/main/notebooks/01_classification/codebooks](https://github.com/jako6f/msc-nlp-therapy-speak/tree/main/notebooks/01_classification/codebooks)

## Sentiment

Sentiment captures whether the local connotational environment of a target term becomes more positive or more negative over time. Following the collocate-based logic of Baes et al. (2024), the measure is computed from words and phrases occurring in a $`\pm 5`$-token window around each target or baseline mention. Unlike Baes et al. (2024), the present study uses NRC–VAD v2.1 valence scores rather than Warriner norms because NRC–VAD provides broader contemporary English coverage and includes multi-word expressions (Mohammad 2025).

For each mention, the focal lexical material itself is removed from the scoring window. The remaining context is tokenised and lemmatised with spaCy `en_core_web_sm`. Punctuation, numerals, and one-character tokens are excluded, but stopwords are retained to keep the collocate index close to Baes et al.’s SIBling implementation. NRC–VAD entries are normalised with the same lemmatisation procedure. Multi-word expressions are matched greedily before unmatched unigram tokens; if several surface entries collapse to the same lemma phrase, their VAD scores are averaged.

For analysis unit $`u`$, publication year $`Y`$, and reported stratum $`s`$, annual sentiment is

```
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

where $`C_{u,Y,s}`$ is the set of NRC–VAD-matched collocates in the local windows, $`f_{w,u,Y,s}`$ is the frequency of collocate $`w`$, and $`V(w)`$ is its NRC–VAD valence score. 

## Intensity

Intensity operationalises vertical concept creep as the decline in the affective arousal of local target contexts. The measure uses the same local-collocates as the sentiment analysis (see above) and differs only in the VAD score being averaged. Consequently, all preprocessing, target-term exclusion, lemmatisation, multi-word matching, frame stratification, and coverage reporting are inherited.

Annual intensity for analysis unit $`u`$, publication year $`Y`$, and reported stratum $`s`$ is

```latex
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

where $`A(w)`$ is the NRC–VAD arousal score for collocate $`w`$. . 

## Breadth

Breadth operationalises horizontal concept creep as contextual dispersion: the more diverse the contexts in which a target term appears, the higher its breadth score. Baes et al. (2024) estimate breadth using *sentence-level* contextual embeddings. This study replaces that generic sentence-embedding representation with XL-LEXEME, a *target-aware word-in-context* (WiC) model designed for lexical semantic change detection (Cassotti et al., 2023). This substitution is methodologically important because ADHD, autism, and the baseline terms are analysed as target uses within local passages, rather than as undifferentiated sentence topics.

For ADHD and autism, no down-sampling is applied: all contexts in the three core substantive frames, together with their aggregate, enter the breadth analysis. Baseline terms are deterministically sampled with a cap of 1,000 contexts per baseline-year, stratified by registered domain to limit domination by high-volume websites.

Each candidate context is marked with explicit XL-LEXEME target delimiters (e.g., “Many <t> autistic </t> adults describe masking in workplace settings.”) The target sentence is used first; if it is too short, the sentence-plus-adjacent context is used instead. Identical marked contexts are encoded once and reused through an embedding index. For each analysis unit, year, and frame stratum, XL-LEXEME produces a contextual embedding of the marked target use. These embeddings are L2-normalised, and breadth is then calculated as the mean pairwise cosine distance among all target-use embeddings in the corresponding cell. 

For analysis unit $`u`$, publication year $`Y`$, and reported stratum $`s`$, with $`N_{u,Y,s}`$ contextual embeddings $`\mathbf{v}_1,\ldots,\mathbf{v}_{N_{u,Y,s}}`$, breadth is

```latex
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

Higher values indicate greater average dissimilarity among target uses in that year and stratum. The implementation uses a closed-form mean-pairwise formula over L2-normalised embedding vectors. This produces the same mean cosine distance as an explicit pairwise distance matrix, but avoids materialising all pairwise distances in memory. Full pairwise distances are therefore generated only for diagnostics, such as inspecting nearest or most dissimilar context pairs.

## Neighbour Similarity Evolution

Baes et al. (2024) operationalise thematic content using a top-down pathologisation dictionary, which is appropriate for terms that can refer either to ordinary affective states or to clinical constructs, such as “anxiety” and “depression”. Because ADHD and autism are diagnostic concepts by definition, the present study instead estimates bottom-up target-neighbour trajectories, following Vylomova and Haslam’s (2021) pairwise similarity time-series of type-level embeddings. This analysis asks which content words become more or less distributionally close to each target concept across publication years.

Models are estimated separately for ADHD and autism stratified by frame. Baseline terms are not modelled because neighbour similarity evolution is used to characterise target-specific thematic associations rather than to produce a scalar target-baseline comparison. The modelling input is the target-centred passage, using document publication year as the time axis. Raw target forms are canonicalised before modelling so that the type-level embedding represents the target concept rather than individual spellings or abbreviations (i.e., ADHD variants are mapped to `adhd_concept`, and autism variants are mapped to `autism_concept`). Passages are lemmatised with spaCy `en_core_web_sm` and filtered to retain content words and canonical concept tokens, while removing punctuation, numerals, stopwords, one- and two-character tokens, and common web-boilerplate tokens. Contexts with fewer than five retained tokens, or with no canonical target token, are excluded.

For each target-frame corpus, a global skip-gram Word2Vec model is trained with 200-dimensional vectors, a context window of 10 tokens, a minimum corpus count of 5, and 10 training epochs, following Vylomova and Haslam (2021). Annual models are then initialised from the corresponding global model and further trained for 10 epochs on passages from each publication year. This gives the annual models a shared target-frame starting space while still allowing yearly neighbour associations to vary.

For target concept (w_c), candidate neighbour (w_j), target unit (u), frame stratum (f), and publication year (t), the neighbour-similarity score is defined as the cosine similarity between the annual embedding of the canonical target concept and the annual embedding of the candidate neighbour:

```latex
\begin{equation}
\operatorname{NSE}_{u,f,t}(w_j)
=
\cos\!\left(
\mathbf{v}^{(u,f,t)}_{w_c},
\mathbf{v}^{(u,f,t)}_{w_j}
\right)
=
\frac{
\mathbf{v}^{(u,f,t)}_{w_c} \cdot \mathbf{v}^{(u,f,t)}_{w_j}
}{
\lVert \mathbf{v}^{(u,f,t)}_{w_c} \rVert
\lVert \mathbf{v}^{(u,f,t)}_{w_j} \rVert
}.
\end{equation}
```

Annual neighbours are extracted only when the canonical target token occurs at least 20 times in the relevant target-frame-year corpus. Candidate neighbours must occur at least five times in the same annual corpus, and the five most similar eligible neighbours are retained for each model. To reduce noise in the resulting trajectories, a neighbour must appear in the annual top-five list in at least two years and have finite similarity estimates in at least ten of the thirteen publication years.

## Statistical Analysis

Annual estimates are the primary objects of interpretation. For all semantic analyses uncertainty intervals are estimated by document-level bootstrap resampling within each analysis-unit, year, and frame-stratum cell, using 500 bootstrap repetitions and the 2.5th and 97.5th percentiles of the bootstrap distribution. The document is the resampling unit because multiple mentions and collocates from the same document are not independent. Reported results refer to 2-tailed tests with no correction for repeated testing.

Residual autocorrelation is checked with a Durbin–Watson-style diagnostic. When a scalar series is flagged, an AR(1)-transformed sensitivity slope is reported in the corresponding trend table; otherwise the ordinary least-squares (OLS) estimate remains the main summary. Quadratic fits are computed for annual series whose linear-model residual diagnostic was flagged for autocorrelation.

# Results

# Discussion

- ADHD is a relatively novel disorder (introduced since 1970); Autism is a more traditional disorder by comparison (Iacob & Uban 2026)
    - may explain differences in salience, and other trajectories

# Conclusion