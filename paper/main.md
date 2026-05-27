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

The following literature review first synthesises empirical evidence on lexical semantic change (LSC) in mental-health-related terms over recent decades, before reviewing the computational approaches used to study these processes and identifying the gap addressed by this project.

# Literature Review

Lexical semantic change (LSC) refers to shifts in a word’s meaning while its grammatical function remains stable, and constitutes a common form of language change (Campbell 2013). For example, cloud, initially a meteorological term, broadened in usage to refer to internet-based data storage.

*Concept Creep Theory* (CCT) posits that harm-related concepts are particularly prone to LSC. Many harm-related terms, including addiction, bullying, harassment, prejudice, and trauma, appear to have expanded in meaning since at least the 1970s (Haslam 2016; Haslam et al. 2020). Concept creep distinguishes between two forms of LSC that can occur concurrently: vertical creep and horizontal creep. Vertical creep refers to the loosening of definitions to include milder instances, whereas horizontal creep refers to the extension of definitions to encompass qualitatively new phenomena.

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

# Materials

## Common Crawl

Common Crawl is the largest freely available public archive of web crawl data and is released in recurring crawl snapshots (Baack 2024; Common Crawl Team 2024). It is widely used as a web-scale source of language data for search, corpus construction, NLP research, and large language model pretraining (Baack 2024; Wenzek et al. 2019). For this project, its central value lies in breadth: Common Crawl provides repeated cross-sections of general web discourse rather than data from a single platform, newspaper archive, or curated clinical corpus. However, it should not be treated as a representative survey of the web or of public opinion. The resulting data reflect what Common Crawl crawled, retained, and made available.

<figure id="fig:commoncrawl-pipeline" data-latex-placement="htbp">
<embed src="commoncrawl_pipeline/commoncrawl_collection_pipeline.pdf" />
<figcaption>Common Crawl collection pipeline used to construct the trend and corpus materials.</figcaption>
</figure>

To collect the data for this study, we built a Common Crawl collection pipeline designed to extract high-quality general discourse for the analysis of diachronic lexical-semantic change in specific target terms, here ADHD and autism, against matched baseline terms. The collection workflow is illustrated in Figure <a href="#fig:commoncrawl-pipeline" data-reference-type="ref" data-reference="fig:commoncrawl-pipeline">3.1</a>. Target and baseline terms are processed together so that yearly denominators, sampling logic, and quality filters remain comparable across term groups. The pipeline uses Common Crawl’s two main file formats sequentially. WET files provide compact extracted plaintext and are therefore used for large-scale term scanning and yearly prevalence denominators, but they lack the HTML structure and metadata needed for stronger validation. Candidate documents are therefore resolved to their corresponding WARC records, which preserve the archived web response, including HTML and headers. Although WARC processing is slower, it enables main-text extraction, term-survival validation, and metadata recovery. This WET-first, WARC-second design keeps the pipeline economical by reserving expensive WARC processing for candidate documents only.

The design consists of two linked tracks. The trend track uses fixed-effort annual samples to estimate how frequently target and baseline terms appear over time. The corpus track builds a larger, quality-gated document corpus for downstream NLP analysis. To reduce the risk that a small number of large websites dominate the corpus, domain caps are applied at 50 WET-validated hit rows per registered domain per Common Crawl crawl. Intermediate summaries and manifests are retained so that each year, crawl, track, and batch remains auditable. The pipeline is designed to run end-to-end on AWS EC2, using S3 as the durable storage and transfer layer for intermediate and final collection artefacts. Yearly crawl selection is deterministic: one Common Crawl snapshot is selected per year near a fixed annual anchor date and then frozen in a crawl map.

The data collection spans 13 annual Common Crawl snapshots from 2014 to 2026. In the trend track, the pipeline scanned 27.7 million WET records \[provisional\] and retained 78,899 WARC-validated term hits \[provisional\]. In the corpus track, it scanned 28.1 million WET records \[provisional\] and retained 42,975 analysis-ready documents \[provisional\]. Target-term coverage comprises 10,998 target documents \[provisional\], including 3,907 ADHD documents \[provisional\] and 8,558 autism documents \[provisional\]. The collection was run on an AWS `m7i-flex.large` instance, featuring 2 vCPUs, 8 GiB of RAM, and up to 12.5 Gbps network bandwidth (AWS, n.d.). On this instance type, corpus throughput was approximately one hour per million scanned WET records \[provisional\].

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

# Method

## Salience (Prevalence)

Using the Trend Track, yearly mention rates will be estimated as relative frequencies (candidate and validated hits per scanned corpus size), enabling a diachronic test of whether these terms are becoming more prominent in general web discourse. *\[status: incomplete\]*

## Intensity (Vertical Creep)

Collocates within a $`\pm 5`$-word window of the target term will be extracted per year and scored using the NRC Valence–Arousal–Dominance lexicon (M. Saif, 2025). Annual weighted arousal indices (and derived “severity” composites, where appropriate) will test whether the surrounding language becomes more or less emotionally intense over time. *\[status: incomplete\]*

## Breadth (Horizontal Creep)

Breadth will be estimated via contextual dispersion: a fixed number of sentences containing the target term will be sampled per year, embedded with transformer sentence embeddings, and summarised by average pairwise cosine distance. Increasing dispersion indicates a widening range of contexts (semantic broadening). *\[status: incomplete\]*

## Sentiment (Connotation)

Using the same collocate windows, annual weighted valence indices (NRC VAD) will test for shifts toward more positive vs more negative connotations (amelioration vs degeneration), reflecting potential destigmatisation or increased pejoration. *\[status: incomplete\]*

## Thematic Evolution

The Corpus Track will be analysed with BERTopic (transformer embeddings, UMAP + HDBSCAN clustering, c-TF-IDF representations) to trace topics-over-time in annual bins. *\[status: incomplete\]*

## Analytic Strategy

Linear regression analyses were performed to test the statistical significance of the predicted trends in the hypotheses....

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
