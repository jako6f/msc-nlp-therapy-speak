<div class="titlepage">

**Tracing Semantic Change in ADHD and Autism Discourse at Web Scale**

Jakob Lütkemeier

MSc Dissertation submitted in partial fulfilment of the requirements for
the degree of

MSc in Applied Social Data Science

[School of Social Sciences and Philosophy](https://www.tcd.ie/ssp/)

[Department of Political Science](https://www.tcd.ie/Political_Science/)

Supervisor: Dr Tom Paskhalis

Trinity College Dublin

10 August 2026

Word count: 11,099

*Note: Student ID is intentionally not printed (university guidance).*

</div>

## Declaration

I hereby declare that this MSc Dissertation is entirely my own work and
that it has not been submitted as an exercise for a degree at this or
any other university.

I have read and I understand the plagiarism provisions in the General
Regulations of the University Calendar for the current year, found at
<http://www.tcd.ie/calendar>.

I have also completed the Online Tutorial on avoiding plagiarism “Ready
Steady Write”, located at
<http://tcd-ie.libguides.com/plagiarism/ready-steady-write>.

Signed: 

------------------------------------------------------------------------

Date: 

------------------------------------------------------------------------

## Ethics and Data-Handling Statement

This study involved no human participants, no interaction or
intervention, and no primary data collection; it is a secondary analysis
of publicly accessible web pages archived and distributed by Common
Crawl. The corpus was obtained in accordance with Common Crawl’s terms
of use, and the archive itself honours publishers’ `robots.txt`
exclusions; no attempt was made to access or reconstruct content
excluded from the crawl. Although the analysed texts include
first-person accounts of mental health experiences, all analyses were
conducted at the level of aggregate annual corpus statistics. No
individuals were identified or profiled, no author- or user-level
inferences were drawn, and no personal identifiers were extracted or
analysed beyond their incidental presence in archived page text. Any
example passages shown are brief and non-identifying. Processing
followed the data-protection principles of purpose limitation and data
minimisation: collection artefacts were stored on access-controlled
cloud infrastructure (AWS S3/EC2) and used solely for this research, and
the released reproducibility materials comprise code, configuration, and
derived aggregate outputs, with the underlying web documents referenced
by their Common Crawl identifiers rather than redistributed.

## Statement on the Use of Artificial Intelligence

Generative AI tools were used in three capacities in this project.
First, as coding assistants: OpenAI’s ChatGPT Codex was used to help
implement and debug parts of the software developed for this study, most
extensively the Common Crawl text-extraction pipeline; all code was
specified, reviewed, tested, and validated by the author, who takes full
responsibility for its correctness. Second, as research instruments: the
frame-annotation workflow described in the Methods chapter uses large
language models by design (OpenAI Codex GPT-5.5 as annotator and Gemini
3 Flash Preview as critic), with human adjudication authoritative
throughout; this methodological use is documented in full in
Section <a href="#sec:methods-frame-classification" data-reference-type="ref"
data-reference="sec:methods-frame-classification">4.2</a>. Third, as a
language aid: bar minor polishing of prose, grammar and wording, all
text was written by the author. AI tools were not used to search,
select, or review the literature, and all interpretations and
conclusions are the author’s own. The author accepts full responsibility
for the entire content of this dissertation.

# Abstract

## Acknowledgements

I would like to thank my supervisor, Dr Tom Paskhalis, for guidance
throughout this dissertation, and the MSc in Applied Social Data Science
teaching team for methodological training and support.

# Introduction

It is widely recognised that many countries are experiencing an
unfolding mental health crisis. Evidence of escalating mental health
complaints comes from surveys, epidemiological studies, and rising
prescriptions of psychiatric medications . Particularly substantial
increases in prevalence have been reported for the neurodevelopmental
disorders autism and attention-deficit hyperactivity disorder (ADHD) .
In Australia, for example, the number of people with autism increased by
42% from 2018 to 2022, including a 95% increase among women and girls .
In the United Kingdom, ADHD diagnoses increased by factors of two,
three, and more than 10 for boys, girls, and adults, respectively, from
2000 to 2018 .

Concurrently, mental health awareness campaigns have proliferated , and
mental health-related discourse has become increasingly common in
everyday life . Again, ADHD and autism are particularly stark examples.
In June 2026, ADHD and autism had been hashtagged in more than 5.4
million and 4 million videos on TikTok, respectively. An analysis of a
LexisNexis dataset revealed that from January to May 2024, ADHD was the
subject of 25,080 media articles, compared with 5,775 articles during
the equivalent period in 2014 .

On the one hand, this popularisation of mental health-related discourse
in everyday life has led to undeniably positive outcomes. It may give
people language for subtle emotional states , improve mental health
literacy , and thereby reduce stigma around mental illness . It may also
support better recognition and more accurate reporting of mental health
problems , erode barriers to help-seeking that contribute to the ongoing
under-treatment of some conditions , and facilitate online identity and
community formation, particularly around ADHD and autism .

On the other hand, the concurrence of rising prevalence rates of mental
ill health and the proliferation of mental health-related discourse in
everyday life has prompted scholars to interrogate the relationship
between the two . term this account the *prevalence inflation
hypothesis*, which posits a cyclical, mutually reinforcing dynamic. On
one side of the cycle, rising prevalence may promote the permeation of
mental health terminology into mainstream discourse. On the other, the
increased availability of such discourse may itself inflate prevalence
by encouraging overdiagnosis, overinterpretation, or pathologisation,
whereby milder or more transient forms of distress are understood and
reported as clinical mental health problems, often in conjunction with
self-diagnosis .

The mechanism is not merely linguistic. Once individuals interpret and
label their psychological experiences as symptoms of a mental health
condition, this may alter self-concept and behaviour . In stronger
cases, such labelling may become self-fulfilling: ordinary distress,
reframed as symptomatic of disorder, may lead to patterns of attention,
avoidance, identification, or help-seeking that intensify the very
symptoms being labelled . This process may be further amplified where
mental health problems are glamorised or romanticised, that is,
represented as socially desirable, identity-conferring, aesthetically
meaningful, or markers of depth and authenticity. Under these
conditions, diagnostic and quasi-diagnostic labels may acquire social
value, making self-labelling attractive rather than merely explanatory .

These concerns are particularly relevant to ADHD and autism, where
clinicians have reported increases in self-diagnosed presentations .
They may also be especially pronounced among adolescents, given their
heightened susceptibility to rumination, peer influence, identity
formation, social reward, and media messaging .

Another corollary of the diffusion of mental health language into
popular culture is a phenomenon described as *therapy-speak*, “the
imprecise and superficial integration of psychotherapy language into
everyday communication” . The consequences are twofold. Firstly,
therapy-speak may spread inaccurate psychological information. An
analysis of the 100 most popular TikTok videos about ADHD found that
more than half (55%) of the characteristics attributed to ADHD by video
creators did not align with the diagnostic criteria of the fifth
revision of the *Diagnostic and Statistical Manual of Mental Disorders*
(DSM-5) . Similarly, an analysis of the 133 most-viewed TikTok videos
tagged \#autism found that only 27% were rated as accurate, 41% as
inaccurate, and 32% as containing potentially misleading
overgeneralisations . Secondly, therapy-speak may erode the meaning and
relevance of mental-health-related terms, a process that can also be
understood as *trivialisation*. This can contribute to hermeneutical
injustice by depriving people who actually live with a certain mental
condition of the words they need to describe their experiences and by
reducing their symptoms to mere personality traits, thereby denying them
a fully recognised psychiatric identity .

This latter concern—the erosion of meaning in psychotherapy terms—is the
focus of the present study, which investigates lexical semantic change
(LSC) in the terms ADHD and autism in general web discourse. The
following literature review first synthesises empirical evidence on LSC
in mental-health-related terms over recent decades, before reviewing the
computational approaches used to study these processes and identifying
the gap addressed by this project.

# Related Work

Lexical semantic change (LSC) refers to shifts in a word’s meaning while
its grammatical function remains stable, and constitutes a common form
of language change . For example, *mouse*, initially a word for a small
rodent, broadened in usage to refer to a computer input device.

*Concept creep theory* posits that harm-related concepts are
particularly prone to LSC, specifically gradual semantic broadening. As
such, many harm-related terms, including *addiction*, *bullying*,
*harassment*, *prejudice*, and *trauma*, are reported to have expanded
in meaning since at least the 1970s . Concept creep distinguishes
between two forms of LSC that can occur concurrently: vertical creep and
horizontal creep. Vertical creep refers to the loosening of definitions
to include milder instances (declining semantic severity or intensity),
whereas horizontal creep refers to the extension of definitions to
encompass qualitatively new phenomena (semantic broadening). Concepts of
mental illness are considered harm-related because distress and
dysfunction are fundamental to their definition . Previous studies have,
therefore, examined concept creep across different mental-health-related
concepts.

found that *trauma* and *addiction* (together with three non-mental
health-related concepts: *bullying*, *prejudice*, and *harassment*)
exhibited both vertical and horizontal creep in a corpus of
approximately 800,000 psychology article abstracts from 875 journals
dating back to the 1960s. Similar, though weaker, patterns were observed
in a general-language corpus combining the Corpus of Contemporary
American English (CoCA) and the Corpus of Historical American English
(CoHA) from the 1970s to the 2010s. Subsequent studies using the same
corpora and time periods have reported comparable findings for other
mental-health-related concepts. found evidence of vertical creep in
*trauma* in the psychology corpus. reported vertical creep in
*addiction*, *anger*, *stress*, and *worry* in the psychology corpus,
and in *addiction*, *grief*, *stress*, and *worry* in the general
corpus. further found that the broader concepts *mental health* and
*mental illness* both expanded horizontally in the psychology corpus.

Not all findings align with this pattern. One study reported that the
semantic intensity of *anxiety* and *depression* increased in both
Vylomova and Haslam’s psychology corpus and their general-language
corpus, contrary to concept-creep expectations . This unexpected result
may reflect measurement unspecificity, particularly the absence of
discourse-context and construct distinctions. For example, *depression*
may refer to psychiatric depression, but also to meteorological or
economic phenomena. Similarly, *anxiety* may refer to a nosological
category, a disorder construct, or an underlying human experience. In a
replication study, showed that this apparent increase in semantic
intensity could be attributed to changing discourse composition,
specifically shifts in the balance between clinical or nosological
contexts and lived-experience contexts, rather than to intrinsic
semantic change alone. In their analysis of lead paragraphs from *New
York Times* articles published between 1970 and 2023, the time effect
for *depression* became nonsignificant after controlling for
mental-health context.

Another study examined a range of terms commonly associated with
therapy-speak, including *toxic*, *bipolar*, *psychopath*,
*narcissistic*, and *triggered*, and reported mixed results . In an
extension of Vylomova and Haslam’s psychology corpus, most terms
exhibited horizontal creep. By contrast, in two Reddit corpora
comprising comments from psychology-oriented subreddits and general
subreddits between 2010 and 2025, most terms showed a narrowing in
breadth. Overall, the authors found that long-established psychological
terms such as *OCD*, *bipolar*, and *trauma* displayed little semantic
change, whereas terms such as *gaslighting* and *imposter* shifted
substantially from year to year.

On balance, many mental-health-related concepts appear to have undergone
concept creep, becoming milder and broader over recent decades. Haslam
and colleagues theorise several drivers of this process . One is the
influence of “opprobrium entrepreneurs”, who seek to cast previously
accepted conditions in a more problematic light; by broadening a harm
concept, the disapproval associated with its original meaning can come
to apply to less severe cases. A second driver is “prevalence-induced
conceptual change”, whereby the standards for recognising harm tend to
loosen as instances of harm become less common. Finally, Haslam and
colleagues point to a broader cultural increase in concern with harm. As
harm concepts expand, a wider range of experiences is treated as morally
significant and worthy of care, and a greater number of actions come to
be seen as harmful .

Whether the broadening of mental-health constructs reflects changes in
official diagnostic criteria is unclear. A meta-analysis showed that no
revision of the *Diagnostic and Statistical Manual of Mental Disorders*
(DSM) from the third edition onward was reliably more inflationary or
deflationary overall. However, specific disorders changed significantly.
Most notably, ADHD inflated by 18%, 33%, and then 17% across the three
revisions following DSM-III. Autism also inflated by 50% from DSM-III to
DSM-III-R, albeit based on a single study, but deflated by 15% from
DSM-IV to DSM-5 .

Computational Approaches to Studying\
Lexical Semantic Change
-------------------------------------

Semantic change has long been studied across linguistics and the social
sciences, but lexical semantic change remains difficult to characterise
because changes in word meaning are often gradual and less visible than
other forms of linguistic change, such as those produced by spelling or
grammar reforms . Earlier research largely depended on manual methods,
with linguists using historical texts, dictionaries, and corpora to
reconstruct how word meanings changed over time. More recently,
computational linguistics and natural language processing (NLP) have
made it possible to study semantic change at much larger scales . Within
NLP, meaning is typically understood in distributional terms: a word’s
meaning is inferred from the contexts in which it appears. These
contextual patterns can be represented through several computational
approaches, including frequency-based measures, topic models, semantic
graphs, and embedding-based methods .

Although considerable progress has been made in identifying LSC using
these techniques , less attention has been paid to characterising the
nature of such changes. To address this gap, proposed the SIBling
framework—Sentiment, Intensity, and Breadth—as a unified,
multidimensional approach to characterising LSC. The framework situates
concept creep within a broader account of LSC and links this conceptual
model to computational methods, including frequency-based and
embedding-based techniques. It distinguishes three dimensions along
which terms may change over time: vertical drift, horizontal drift, and
sentiment. Vertical drift refers to changes in intensity. A term may
strengthen, as in *hilarious* shifting from cheerful or amusing to
extremely funny, or weaken, as in *trauma* shifting from brain injuries
to milder events such as business loss. This dimension maps onto
vertical concept creep. Horizontal drift refers to changes in breadth. A
term may narrow, as in *doctor* shifting from scholar or teacher to
primarily denoting a medical professional, or broaden, as in *cloud*
shifting from a meteorological term to internet-based data storage. This
dimension maps onto horizontal concept creep. Sentiment refers to
changes in connotation. A term may acquire a more positive connotation,
as in *geek* shifting from a derogatory term for odd people to someone
passionate about a field, or a more negative connotation, as in
*retarded* shifting from a neutral term for intellectual disability to a
highly pejorative slur. This dimension roughly corresponds to
destigmatisation and stigmatisation, which are not directly captured by
concept creep theory. According to SIBling, these dimensions can be
complemented by examining shifts in target-word frequency, or salience,
and in the thematic content of target-word collocates. Despite its
conceptual value, however, the framework’s operationalisations should be
treated as first implementations rather than settled measures: they have
not yet been extensively validated externally, and their measurement
choices remain open to refinement .

## The Present Study

Three gaps in the existing literature motivate the present study.

The first concerns the evidentiary basis of prior work. Research on LSC
in mental-health-related concepts has relied heavily on a small set of
curated corpora, particularly COCA/COHA and Vylomova and Haslam’s
psychology abstracts datasets . This matters because the broader aim of
this literature is to infer cultural dynamics from lexical change. Yet
curated corpora may partly reflect shifts in editorial policy, audience
composition, disciplinary conventions, or ideological stance rather than
broader changes in public discourse . To date, LSC in
mental-health-related concepts has not been examined systematically in
general web discourse.

The second concerns the target concepts themselves. Direct evidence on
LSC in ADHD and autism remains limited. One study found that the two
concepts have converged semantically on Reddit: from 2019 onward, their
contextual similarity increased, and the terms became more similar to
each other than to comparison conditions . This finding, however,
captures convergence between ADHD and autism on a single platform; it
does not characterise how either concept’s semantic profile has evolved
over time in general web discourse.

The third concerns discourse composition. Existing work indicates that
apparent trends in LSC may partly reflect shifts in the contexts in
which terms are used—such as clinical categories, service labels,
identity or lived-experience categories, and objects of public
debate—rather than intrinsic semantic change . Treating all mentions as
a single semantic population therefore risks conflating genuine semantic
change with change in discourse composition.

Taken together, these gaps point to three corresponding requirements: a
broader evidentiary base that captures general web discourse, direct
diachronic analysis of ADHD and autism, and an analytic approach that
accounts for variation in discourse framing. The present study was
designed to meet these requirements.

Guided by these requirements, the present study addresses four research
questions:

RQ1:  
How has the salience of ADHD and autism in general web discourse changed
from 2014 to 2026, against the backdrop of non-clinical negative-emotion
baseline terms?

RQ2:  
How has the balance between clinical and lived-experience framing of
ADHD and autism changed over this period?

RQ3:  
How have the intensity, breadth, and sentiment of ADHD and autism
contexts changed—overall and within clinical versus lived-experience
frames—against the backdrop of the baseline terms?

RQ4:  
How has the thematic content of ADHD and autism discourse evolved over
time?

Consistent with concept creep theory, it is hypothesised that ADHD and
autism will increasingly appear in less emotionally intense contexts
(vertical concept creep) and across a broader qualitative range of
lexical contexts (horizontal concept creep). This hypothesis corresponds
to the intensity and breadth components of RQ3; the remaining
measures—sentiment (RQ3), frame composition (RQ2), and thematic content
(RQ4)—are treated as exploratory, given the limited prior research
available to motivate directional predictions.

Methodologically, the present study adapts Baes et al.’s SIBling
framework for LSC analysis, refining its operationalisation through
target-aware embeddings for breadth and an expanded affective lexicon
for intensity and sentiment. To broaden the evidentiary base and support
direct diachronic semantic analysis of ADHD and autism, the study
constructs a diachronic corpus of general web discourse using Common
Crawl, a large-scale, openly accessible repository of web crawl data.
The corpus spans 2014–2026, from the earliest period with sufficiently
consistent and usable data to the most recent available year. An
efficient and reproducible pipeline extracts quality-filtered web
documents containing ADHD, autism, and baseline emotion terms,
supporting both frequency estimation and downstream semantic analysis
while ensuring comparability across targets and baselines. Finally, to
account for variation in discourse framing, the study incorporates a
supervised frame-classification step that distinguishes clinical or
disorder-construct discourse from lived-experience discourse prior to
LSC analysis.

In sum, the present study contributes to the literature by extending
concept creep and therapy-speak research to ADHD and autism; shifting
the evidentiary base for LSC research from curated corpora toward
general web discourse; introducing a frame-aware approach for separating
clinical and lived-experience uses of mental health terms; implementing
a reproducible Common Crawl pipeline for LSC research; and assessing
whether selected operational refinements can strengthen the application
of the SIBling framework.

# Data and Materials

## Common Crawl

Common Crawl is the largest freely available public archive of web crawl
data, totalling more than 10 petabytes across crawls published
approximately monthly, each typically containing more than two billion
web pages . Widely used as a web-scale source of language data for
corpus construction, NLP research, and large language model pretraining
, it has been cited in over 12,000 research papers . For the present
study, its central value lies in breadth: rather than drawing on a
single platform, newspaper archive, or curated corpus, Common Crawl
provides repeated cross-sections of general web discourse. The archive
stores raw HTML and, importantly, collects only a sample of pages from
each domain it visits—meaning, for instance, that only a subset of
Wikipedia articles will appear, not the full site. Domain selection and
page depth are governed by *harmonic centrality*, a graph-based scoring
method adopted in 2017 whereby a domain’s importance is determined by
the volume of direct and indirect inbound links from other domains, with
direct links weighted most heavily; higher-scoring domains are both more
likely to be crawled and to have more of their pages fetched . Despite
its enormous size, Common Crawl is neither a complete copy of the web
nor a representative sample of it. A growing number of rights
holders—including major outlets such as the New York Times—now block
Common Crawl via the `robots.txt` standard, largely in response to AI
training data concerns, and large social media platforms such as
Facebook have done so for considerably longer . The data used in this
study should therefore not be treated as a representative survey of the
web or of public opinion; they reflect what Common Crawl crawled,
retained, and made available within these structural constraints.

#### Common Crawl Collection Pipeline

<figure id="fig:commoncrawl-pipeline" data-latex-placement="!htbp">
<embed src="commoncrawl_pipeline/commoncrawl_collection_pipeline.pdf" />
<figcaption>Common Crawl collection pipeline used to construct the trend
and corpus materials.</figcaption>
</figure>

To collect the data for this study, a Common Crawl collection pipeline
was developed to extract quality-gated general discourse for the
analysis of lexical semantic change (LSC) in specific target terms, here
ADHD and autism, against matched baseline terms. The pipeline workflow
is illustrated in
Figure <a href="#fig:commoncrawl-pipeline" data-reference-type="ref"
data-reference="fig:commoncrawl-pipeline">3.1</a>. Target and baseline
terms are processed together so that yearly denominators, sampling
logic, and quality filters remain comparable across term groups. The
pipeline uses Common Crawl’s two main file formats sequentially. WET
files provide compact extracted plaintext and are therefore used for
large-scale term scanning and yearly prevalence denominators, but they
lack the HTML structure and metadata needed for stronger boilerplate
filtering. Candidate documents are therefore resolved to their
corresponding WARC records, which preserve the full HTML code. Although
WARC processing is slower, it enables main-text extraction, boilerplate
removal, and metadata recovery. This WET-first, WARC-second design keeps
the pipeline economical by reserving expensive WARC processing for
candidate documents only.[^1]

The design consists of two linked tracks. The trend track uses
fixed-effort annual samples to estimate how frequently target and
baseline terms appear over time. The corpus track builds a larger,
quality-gated document corpus for downstream NLP analysis. As an
additional safeguard against overrepresentation by large websites beyond
Common Crawl’s restrictive sampling approach, domain caps are applied at
50 WET-validated hit rows per registered domain per Common Crawl crawl.
Intermediate summaries and manifests are retained so that each year,
crawl, track, and batch remains auditable. The pipeline is designed to
run end-to-end on AWS EC2, using S3 as the durable storage and transfer
layer for intermediate and final collection artefacts. Yearly crawl
selection is deterministic: one Common Crawl snapshot is selected per
year near a fixed annual anchor date and then frozen in a crawl map.

Several software choices are methodologically consequential because they
affect corpus membership. WET and WARC records are parsed with `warcio`,
WARC pointers are resolved through a local `pywb`-based index server,
archived HTML is converted to main text with Trafilatura and
Resiliparse, and post-extraction filtering uses DataTrove quality
filters followed by English-language filtering with `py3langid` .[^2]

The data collection spans 13 annual Common Crawl snapshots from 2014 to
2026. This window maximises temporal depth while remaining compatible
with a stable WET-first, WARC-second workflow. By 2014, Common Crawl
provided sufficiently large WET/WARC-format crawls for efficient
plaintext scanning, denominator construction, and HTML-based validation;
earlier data are less comparable and require additional handling due to
older archive formats. The 2026 endpoint reflects the latest collection
year available for the project. In the trend track, the pipeline scanned
55.6 million web pages and retained 156,189 pages featuring validated
term hits. In the corpus track, it scanned 220 million web pages and
retained 336,178 analysis-ready documents. Of these, 87,173 documents
contain ADHD or autism target terms. Target membership is non-exclusive:
31,354 documents contain ADHD terms, 67,614 contain autism terms, and
11,795 contain both. The collection was run on an AWS `m7i-flex.large`
instance, featuring 2 vCPUs, 8 GiB of RAM, and up to 12.5 Gbps network
bandwidth . On this instance type, corpus throughput was approximately
one hour per million scanned WET records.

## Target Terms

Two target concepts were selected for diachronic semantic analysis:
*ADHD* and *autism*. Target documents were retrieved using the matching
expressions shown in
Table <a href="#tab:target-patterns" data-reference-type="ref"
data-reference="tab:target-patterns">3.1</a>. The abbreviation `ASD`
(autism spectrum disorder) was retained only when *autism* occurred
within $`\pm 200`$ characters, reducing false positives from unrelated
acronym use. The acronym `ADD` (attention deficit disorder) was not
included as a matching expression because ADHD is the current canonical
diagnostic label, whereas ADD is older and colloquial ; more
importantly, `ADD` would create substantial false positives in WET
scanning because it overlaps with the common verb *add* and with web
chrome such as “add to cart”. The broader expression
`attention[-]?deficit` retains coverage of relevant expanded forms while
avoiding this high-noise acronym.

<div id="tab:target-patterns">

| Concept | Matching expressions             |
|:--------|:---------------------------------|
| ADHD    | `̱`; `attention[-]?deficit`       |
| Autism  | `̱`; `̱`; `autism[-]?spectrum`; `̱` |

Target-term matching expressions.

</div>

For comparison, three negative, non-clinical emotion terms were selected
with sufficient coverage and interpretable usage: *frustration*,
*sadness*, and *loneliness*. These terms are not exact semantic controls
for ADHD and autism; they provide a baseline for separating
target-specific change from broader shifts in negative affective
language in the corpus.

## Preprocessing

Following , preprocessing was organised around analysis-specific corpus
representations rather than a single all-purpose text file. The
downstream semantic analyses use a shared mention-level context table
built from the extraction pipeline’s corpus product. Target and baseline
mentions were re-detected using the frozen collection patterns (see
Table <a href="#tab:target-patterns" data-reference-type="ref"
data-reference="tab:target-patterns">3.1</a>), including the ASD
disambiguation rule, and overlapping matches within the same analysis
unit were resolved by keeping the longest span. This prevents
expressions such as *autism spectrum* from being counted twice as both a
phrase and a shorter nested form. Documents containing both ADHD and
autism terms were allowed to contribute to both target groups, with
separate mention-level contexts emitted for each concept. Same-sentence
acronym-and-expansion pairs occurring within a short local window were
also collapsed, so cases such as “attention deficit hyperactivity
disorder (ADHD)” contribute one conceptual ADHD context rather than two
near-duplicate rows. To limit domination by repetitive documents while
preserving repeated-use signal, each document could contribute at most
three mentions per analysis unit. The semantic time variable was then
defined as publication year and only contexts with parseable publication
dates between 2014 and 2026 were retained for the shared LSC table
(retention rate: 97.76%). This yielded 293,670 mention contexts from
212,651 documents and 135,771 registered domains, including 28,611 ADHD
contexts, 68,253 autism contexts, and separate baseline series for
frustration, sadness, and loneliness. For each retained mention, the
table stores the matched form, raw-form collapse diagnostics, registered
domain, publication and crawl metadata, a $`\pm 5`$-token window for
affective collocate analyses, and target-sentence passages for breadth
and thematic analyses.

## NRC–VAD Lexicon

The affective analyses use the NRC Valence, Arousal, and Dominance (VAD)
Lexicon v2.1 , rather than the Warriner norms used in SIBling . NRC–VAD
offers several advantages for the present study. It is substantially
larger, providing human ratings for approximately 45,000 English words
and 10,000 multi-word phrases, compared with 13,915 words and no phrase
entries in Warriner norms. It has also been reported to have higher
reliability, with an aggregate reliability estimate of 0.923 across the
three VAD dimensions, compared with 0.823 for the Warriner norms.

NRC–VAD scores range from $`-1`$ to $`1`$. Higher valence scores
indicate more positive affect, higher arousal scores indicate greater
emotional activation, and higher dominance scores indicate greater
perceived control or power. This study uses valence to estimate whether
the local contexts of target terms become more positive or negative over
time, and arousal to estimate changes in emotional intensity. Dominance
is included in the source lexicon but is not used in the present
analyses. The ratings were produced using Best–Worst Scaling, in which
annotators are shown four items and asked to identify the item that best
and worst represents the property being rated .

# Methods

The analysis adapts Baes et al.’s SIBling framework, which characterises
lexical semantic change (LSC) along interpretable dimensions rather than
treating change as a single aggregate distance. The study estimates
annual trajectories for ADHD and autism discourse across salience, frame
composition, intensity, breadth, sentiment, and neighbour similarity
evolution. Salience measures how often target and baseline terms occur
in sampled Common Crawl slices. Frame composition tracks the balance
between clinical/disorder-construct and lived-experience uses of ADHD
and autism. Intensity and sentiment measure the affective arousal and
valence of local collocates, respectively. Breadth measures contextual
dispersion among target-aware embeddings. Neighbour similarity evolution
tracks how the semantic proximity between each target concept and its
recurrent neighbouring terms changes across the study period. Post-hoc
diagnostics inspect which VAD-matched collocates and high-distance
context words contribute most to the sentiment, intensity, and breadth
results. Salience is indexed by Common Crawl source year, while frame
composition and all semantic analyses use document publication year as
the annual time axis. For ADHD and autism, semantic estimates are
reported overall and separately within clinical/disorder and
lived-experience frames. Baseline terms are not frame-labelled because
the clinical/lived-experience distinction is specific to ADHD and autism
discourse. [^3]

## Salience

Salience estimates whether ADHD and autism terms become more or less
frequent in sampled Common Crawl web discourse over time. Unlike all
semantic analyses which use document publication year, salience is
measured on the Common Crawl source-year axis. This choice is technical
rather than conceptual: the denominator must cover the full yearly WET
sample entering term matching, including pages without target hits, and
publication dates are only recovered downstream for WARC-extracted hit
documents. A publication-year denominator would therefore be unavailable
for the non-hit background corpus. Source year is not identical to
publication year , so salience should be interpreted as source-year
prominence in the sampled crawl rather than as a direct estimate of
publication-year prevalence.

For each analysis unit $`u`$ and Common Crawl source year $`Y`$, the
numerator is the number of WARC-validated term hits, and the denominator
is the number of tokens in minimum-length WET records entering the
annual scan. Reported salience rates are scaled to hits per million WET
tokens:

``` math
\begin{equation}
\operatorname{Salience}_{u,Y}
=
1{,}000{,}000
\times
\frac{H^{\operatorname{WARC}}_{u,Y}}{T^{\operatorname{WET}}_Y}.
\end{equation}
```

## Frame Classification

Frame classification is included before the semantic analyses because
target-term contexts may change not only in meaning but also in
discourse composition. In web text, ADHD and autism may be framed as
diagnoses, disorder constructs, service categories, identities, lived
experiences, community labels, or incidental boilerplate. Treating all
target contexts as one semantic population would risk conflating LSC
with shifts in the prevalence of these frames. This concern follows ,
who show that apparent semantic-intensity trends can be explained by the
changing mental-health context in which a term appears.

The annotation unit is the target sentence plus adjacent sentence
context. Each ADHD/autism passage is labelled hierarchically. First, the
passage is coded for whether it contains substantive target discourse.
Passages that are thin, list-like, navigational, promotional, generic,
incidental, noisy, or otherwise insufficient for target-specific
interpretation are assigned to the non-substantive or insufficient
category. Substantive passages are then coded on two non-exclusive axes:
whether clinical framing is present and whether lived-experience framing
is present. Clinical framing covers diagnosis, disorder status,
symptoms, impairment, treatment, services, medication, research,
epidemiology, DSM/ICD-style categories, and educational or clinical
support needs. Lived-experience framing covers identity,
self-understanding, family or first-person experience, neurodivergent
community, masking, stigma, accommodation, everyday coping, belonging,
pride, and embodied or social experience. The two frame axes are
converted deterministically into five derived strata. Substantive
passages with clinical but not lived-experience framing are labelled
`clinical-only`; passages with lived-experience but not clinical framing
are labelled `lived-only`; passages with both are labelled `mixed`; and
substantive passages with neither are labelled `substantive-other`.[^4]

Frame labels were generated using an Annotation with Critical Thinking
(ACT) workflow, adapted from . Rather than treating LLM labels as final
annotations, ACT uses one model as an annotator, a second model as a
criticiser that estimates which annotations are most likely to be
erroneous, and human adjudication to resolve uncertain or high-risk
cases. First, a 200-passage human pilot was used to refine the codebook.
The locked codebook was then applied to 3,000 ADHD/autism passages using
OpenAI’s Codex GPT-5.5 with high reasoning effort, producing initial
machine annotations for the full annotation batch. Gemini 3 Flash
Preview, accessed via the Gemini CLI, then reviewed each Codex-generated
annotation and assigned an error-risk score. Following ACT, human review
was concentrated on the highest-risk cases using a threshold-based
selection rule, rather than distributed randomly across the full batch.
Human correction remained authoritative throughout: critic outputs were
used only to prioritise review, and labels were changed only after
manual inspection. This procedure preserved human control over difficult
boundary cases while reducing the amount of fully manual annotation
required. To guard against critic blind spots, a random sample of
lower-risk cases was also inspected.

The corrected labels were used to train a hierarchical classifier over
`all-MPNET-base-v2` passage embeddings. The classifier uses three
balanced logistic-regression heads with standardised features: one head
predicts substantive target discourse for all labelled examples, while
the clinical and lived-experience heads are trained only on substantive
examples. Year, URL, and domain metadata are excluded from the
classifier features to reduce leakage from temporal or source-specific
artefacts. A separate 200-passage human validation set was held out from
codebook development, LLM annotation, criticism, correction, and model
training. After validation, the classifier was applied to all
ADHD/autism contexts, producing hard frame labels and frame
probabilities for downstream analysis.

## Sentiment

Sentiment captures whether the local connotational environment of a
target term becomes more positive or more negative over time. Following
the collocate-based logic of , the measure is computed from words and
phrases occurring in a $`\pm 5`$-token window around each target or
baseline mention. Unlike , the present study uses NRC–VAD v2.1 valence
scores rather than Warriner norms because NRC–VAD provides broader
contemporary English coverage and includes multi-word expressions .

For each mention, the focal lexical material itself is removed from the
scoring window. The remaining context is tokenised, part-of-speech
tagged, and lemmatised with spaCy `en_core_web_sm`. Following the
preprocessing logic of , uninformative tokens are excluded before
scoring: punctuation, symbols, spaces, particles, numerals,
non-alphabetic material, one-character lemmas, and spaCy stopwords are
removed. NRC–VAD entries are normalised with the same lemmatisation and
filtering procedure; multi-word lexicon entries are retained only when
all component tokens pass the collocate filter. Multi-word expressions
are matched greedily before unmatched unigram tokens; if several surface
entries collapse to the same lemma phrase, their VAD scores are
averaged.

For analysis unit $`u`$, publication year $`Y`$, and reported stratum
$`s`$, annual sentiment is

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

where $`C_{u,Y,s}`$ is the set of NRC–VAD-matched collocates in the
local windows, $`f_{w,u,Y,s}`$ is the frequency of collocate $`w`$, and
$`V(w)`$ is its NRC–VAD valence score. A post-hoc contributor diagnostic
groups target-frame observations into early, middle, and late periods
and ranks collocates by their frequency-weighted positive and negative
valence contributions.

## Intensity

Intensity operationalises vertical concept creep as the decline in the
affective arousal of local target contexts. The measure uses the same
local-collocates as the sentiment analysis (see above) and differs only
in the VAD score being averaged. Consequently, all preprocessing,
target-term exclusion, lemmatisation, multi-word matching, frame
stratification, and coverage reporting are inherited.

Annual intensity for analysis unit $`u`$, publication year $`Y`$, and
reported stratum $`s`$ is

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

where $`A(w)`$ is the NRC–VAD arousal score for collocate $`w`$. The
intensity post-hoc diagnostic applies the same period grouping and
contributor-ranking procedure to NRC–VAD arousal scores.

## Breadth

Breadth operationalises horizontal concept creep as contextual
dispersion: the more diverse the contexts in which a target term
appears, the higher its breadth score. estimate breadth using
*sentence-level* contextual embeddings. This study replaces that generic
sentence-embedding representation with XL-LEXEME, a *target-aware
word-in-context* (WiC) model designed for LSC detection . This
substitution is methodologically important because ADHD, autism, and the
baseline terms are analysed as target uses within local passages, rather
than as undifferentiated sentence topics.

For ADHD and autism, no down-sampling is applied: all contexts in the
three core substantive frames, together with their aggregate, enter the
breadth analysis. Baseline terms are deterministically sampled with a
cap of 1,000 contexts per baseline-year, stratified by registered domain
to limit domination by high-volume websites.

Each candidate context is marked with explicit XL-LEXEME target
delimiters (e.g., “Many `<t>` autistic `</t>` adults describe masking in
workplace settings.”) The target sentence is used first; if it is too
short, the sentence-plus-adjacent context is used instead. Identical
marked contexts are encoded once and reused through an embedding index.
For each analysis unit, year, and frame stratum, XL-LEXEME produces a
contextual embedding of the marked target use. These embeddings are
L2-normalised, and breadth is then calculated as the mean pairwise
cosine distance among all target-use embeddings in the corresponding
cell.

For analysis unit $`u`$, publication year $`Y`$, and reported stratum
$`s`$, with $`N_{u,Y,s}`$ contextual embeddings
$`\mathbf{v}_1,\ldots,\mathbf{v}_{N_{u,Y,s}}`$, breadth is

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

Higher values indicate greater average dissimilarity among target uses
in that year and stratum. The implementation uses a closed-form
mean-pairwise formula over L2-normalised embedding vectors. This
produces the same mean cosine distance as an explicit pairwise distance
matrix, but avoids materialising all pairwise distances in memory. Full
pairwise distances are therefore generated only for diagnostics, such as
inspecting nearest or most dissimilar context pairs. Because breadth is
not a lexical weighted-average measure, there is no exact collocate
analogue to the sentiment and intensity contributor tables. The post-hoc
breadth diagnostic instead ranks contexts by their average cosine
distance to other contexts in the same target-frame-period cell and
summarises frequent content lemmas among the highest-distance contexts.

## Neighbour Similarity Evolution

operationalise thematic content using a top-down pathologisation
dictionary, which is appropriate for terms that can refer either to
ordinary affective states or to clinical constructs, such as *anxiety*
and *depression*. Because ADHD and autism are diagnostic concepts by
definition, the present study instead estimates bottom-up
target-neighbour trajectories, following the pairwise similarity
time-series of type-level embeddings in . This analysis asks which
content words become more or less distributionally close to each target
concept across publication years.

Models are estimated separately for ADHD and autism stratified by frame.
Baseline terms are not modelled because neighbour similarity evolution
is used to characterise target-specific thematic associations rather
than to produce a scalar target-baseline comparison. The modelling input
is the target-centred passage, using document publication year as the
time axis. Raw target forms are canonicalised before modelling so that
the type-level embedding represents the target concept rather than
individual spellings or abbreviations (i.e., ADHD variants are mapped to
`adhd_concept`, and autism variants are mapped to `autism_concept`).
Passages are lemmatised with spaCy `en_core_web_sm` and filtered to
retain content words and canonical concept tokens, while removing
punctuation, numerals, stopwords, one- and two-character tokens, and
common web-boilerplate tokens. Contexts with fewer than five retained
tokens, or with no canonical target token, are excluded.

For each target-frame corpus, a global skip-gram Word2Vec model is
trained with 200-dimensional vectors, a context window of 10 tokens, a
minimum corpus count of 5, and 10 training epochs, following . Annual
models are then initialised from the corresponding global model and
further trained for 10 epochs on passages from each publication year.
This gives the annual models a shared target-frame starting space while
still allowing yearly neighbour associations to vary.

For target concept $`w_c`$, candidate neighbour $`w_j`$, target unit
$`u`$, frame stratum $`f`$, and publication year $`t`$, the
neighbour-similarity score is defined as the cosine similarity between
the annual embedding of the canonical target concept and the annual
embedding of the candidate neighbour:

``` math
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

Annual neighbours are extracted only when the canonical target token
occurs at least 20 times in the relevant target-frame-year corpus.
Candidate neighbours must occur at least five times in the same annual
corpus, and the five most similar eligible neighbours are retained for
each model. To reduce noise in the resulting trajectories, a neighbour
must appear in the annual top-five list in at least two years and have
finite similarity estimates in at least ten of the thirteen publication
years.

## Statistical Analysis

Annual estimates are the primary objects of interpretation. For all
semantic analyses uncertainty intervals are estimated by document-level
bootstrap resampling within each analysis-unit, year, and frame-stratum
cell, using 500 bootstrap repetitions and the 2.5th and 97.5th
percentiles of the bootstrap distribution. The document is the
resampling unit because multiple mentions and collocates from the same
document are not independent. Reported results refer to two-tailed tests
with no correction for repeated testing.

Residual autocorrelation is assessed using a Durbin–Watson-style
statistic computed on the OLS residuals $`e_t`$:
``` math
\begin{equation}
D=\frac{\sum_{t=2}^{T}(e_t-e_{t-1})^2}{\sum_{t=1}^{T}e_t^2}.
\end{equation}
```
Annual series are flagged when $`D<1.25`$ or $`D>2.75`$. When a scalar
annual series is flagged, a First-Order Autoregressive model, AR(1), is
estimated as a sensitivity check and reported in the appendix;
otherwise, the ordinary least-squares (OLS) slope remains the main
summary . Quadratic fits are computed only for annual series whose
linear-model residual diagnostics indicate autocorrelation, and are
treated as diagnostic checks rather than primary trend estimates.

# Results

This chapter reports diachronic results for salience, frame composition,
sentiment, intensity, breadth, and thematic neighbour similarity for
ADHD and autism. Salience is indexed by Common Crawl source year, while
the semantic measures use document publication year. Annual estimates
and their bootstrap intervals show the observed trajectories; ordinary
least-squares (OLS) models summarise net linear movement over the 13
annual observations. In these models, $`B`$ is the unstandardised change
per year and adjusted $`R^2`$ summarises model fit. The reported
$`p`$-values are descriptive and uncorrected. When linear-model
residuals were flagged for autocorrelation, an AR(1)-transformed
sensitivity model assessed whether the estimated slope persisted after
accounting for first-order serial dependence.

The semantic results foreground the overall aggregate and the two clear,
mutually exclusive frames: clinical-only and lived-experience-only. The
overall aggregate combines clinical-only, lived-experience-only, and
mixed contexts. Mixed contexts are not plotted separately because this
smaller stratum generally occupied an intermediate position between the
overall and clinical trajectories and added visual overlap.
Substantive-other contexts were sparse and outside the main frame
contrast, while non-substantive or insufficient contexts were treated as
a quality and composition category rather than as a semantic frame.

Table <a href="#tab:lsc-regression-target-frames" data-reference-type="ref"
data-reference="tab:lsc-regression-target-frames">5.1</a> reports the
target trend models. At the uncorrected $`p<.05`$ level, autism salience
declined; frame composition shifted toward lived-experience; no target
sentiment OLS slope differed from zero; intensity increased only in
autism clinical contexts; and breadth decreased for ADHD overall, ADHD
clinical contexts, and autism lived-experience contexts. The autism
clinical breadth OLS slope was nominally positive but was not retained
by the AR(1) sensitivity model.

<div id="tab:lsc-regression-target-frames">

| Measure | Target | Frame | $`B(SE)`$ | $`\beta`$ | Adj. $`R^2`$ |
|:---|:---|:---|:--:|:--:|:--:|
| Salience | ADHD | Overall | $`0.0073`$ (0.0043) | 0.46 | 0.14 |
| Salience | Autism | Overall | $`-0.0228^{*}`$ (0.0087) | -0.62 | 0.33 |
| Sentiment | ADHD | Overall | $`0.0023^{\dagger}`$ (0.0014) | 0.46 | 0.14 |
| Sentiment | ADHD | Clinical | $`0.0013^{\dagger}`$ (0.0015) | 0.24 | -0.03 |
| Sentiment | ADHD | Lived experience | $`0.0002`$ (0.0017) | 0.03 | -0.09 |
| Sentiment | Autism | Overall | $`0.0012`$ (0.0006) | 0.50 | 0.18 |
| Sentiment | Autism | Clinical | $`0.0022^{\dagger}`$ (0.0015) | 0.42 | 0.10 |
| Sentiment | Autism | Lived experience | $`-0.0038`$ (0.0022) | -0.46 | 0.14 |
| Intensity | ADHD | Overall | $`0.0001`$ (0.0005) | 0.04 | -0.09 |
| Intensity | ADHD | Clinical | $`0.0002`$ (0.0006) | 0.10 | -0.08 |
| Intensity | ADHD | Lived experience | $`0.0015`$ (0.0009) | 0.45 | 0.13 |
| Intensity | Autism | Overall | $`0.0011`$ (0.0008) | 0.36 | 0.05 |
| Intensity | Autism | Clinical | $`0.0010^{*}`$ (0.0004) | 0.56 | 0.25 |
| Intensity | Autism | Lived experience | $`0.0025`$ (0.0021) | 0.34 | 0.04 |
| Breadth | ADHD | Overall | $`-0.0020^{***}`$ (0.0003) | -0.89 | 0.77 |
| Breadth | ADHD | Clinical | $`-0.0018^{***}`$ (0.0003) | -0.85 | 0.69 |
| Breadth | ADHD | Lived experience | $`-0.0004^{\dagger}`$ (0.0006) | -0.18 | -0.06 |
| Breadth | Autism | Overall | $`0.0001^{\dagger}`$ (0.0002) | 0.14 | -0.07 |
| Breadth | Autism | Clinical | $`0.0007^{*\dagger}`$ (0.0003) | 0.58 | 0.28 |
| Breadth | Autism | Lived experience | $`-0.0010^{*}`$ (0.0004) | -0.60 | 0.30 |

Descriptive annual trend models for ADHD and Autism LSC trajectories by
frame.

</div>

Note. Cells report annual unstandardised OLS slopes as $`B(SE)`$;
$`\beta`$ is the standardised year coefficient. Salience uses Common
Crawl source year and has only an overall target row; semantic measures
use document publication year. Significance markers: $`^{*}p<.05`$,
$`^{**}p<.01`$, $`^{***}p<.001`$. $`^{\dagger}`$ indicates a
residual-autocorrelation flag; AR(1) sensitivity estimates for flagged
rows are reported in Appendix
Table <a href="#tab:lsc-ar1-sensitivity-flagged" data-reference-type="ref"
data-reference="tab:lsc-ar1-sensitivity-flagged">8.3</a>. P values are
descriptive and uncorrected.

## Salience

Figure <a href="#fig:lsc-salience" data-reference-type="ref"
data-reference="fig:lsc-salience">5.1</a> presents WARC-validated
source-year salience. Autism remained more frequent than ADHD throughout
2014–2026, but the fitted salience trajectory declined by 28.9% from
2014 to 2026 ($`B=-0.0228`$, $`p=.024`$, adjusted $`R^2=.33`$). ADHD
moved in the opposite direction, with a fitted 27.0% increase over the
same period, although the annual slope did not differ from zero
($`B=0.0073`$, $`p=.118`$, adjusted $`R^2=.14`$). The indexed panel
shows the target trajectories relative to the comparators; comparator
trend models are reported in Appendix
Table <a href="#tab:lsc-regression-baseline-comparators"
data-reference-type="ref"
data-reference="tab:lsc-regression-baseline-comparators">8.2</a>.

<figure id="fig:lsc-salience" data-latex-placement="!htbp">
<embed src="lsc/salience/lsc_salience_primary_trajectories.pdf" />
<figcaption>WARC-validated ADHD and autism hits per million WET tokens
by Common Crawl source year. The right panel indexes each target and
comparator series to its 2014 rate.</figcaption>
</figure>

## Frame Classification

Table <a href="#tab:lsc-classification-validation" data-reference-type="ref"
data-reference="tab:lsc-classification-validation">5.2</a> shows that
the hierarchical classifier performed well on held-out passages, with
strongest performance for substantive-context detection and clinical
framing, and weaker but still usable performance for lived-experience
framing. The substantive head was evaluated on all 200 passages and
reached $`F_1=.861`$ and balanced accuracy $`=.791`$. The clinical and
lived-experience heads were evaluated conditionally on the 141 passages
labelled as substantive by the human coder. Their $`F_1`$ scores were
.894 and .760, with balanced accuracies of .804 and .829, respectively.

<div id="tab:lsc-classification-validation">

| Validation target            | Support | Precision | Recall |  F1  | Accuracy | Bal. acc. |
|:-----------------------------|:-------:|:---------:|:------:|:----:|:--------:|:---------:|
| Substantive target discourse | 141/200 |   .887    |  .837  | .861 |   .810   |   .791    |
| Clinical framing             | 105/141 |   .903    |  .886  | .894 |   .844   |   .804    |
| Lived-experience framing     | 46/141  |   .704    |  .826  | .760 |   .830   |   .829    |

Held-out validation performance for the hierarchical frame classifier.

</div>

Note. Support gives positive cases over the validation denominator.
Substantive target discourse is evaluated on all 200 held-out passages.
Clinical and lived-experience performance is evaluated among the 141
passages labelled as substantive target discourse by the human coder.

The classifier labelled 96,864 target contexts: 28,611 for ADHD and
68,253 for autism. Clinical-only contexts comprised 42.1% of ADHD and
29.8% of autism contexts; lived-experience-only contexts comprised 12.6%
and 18.8%, respectively. The remaining assignments were mixed (7.0% and
9.3%), non-substantive or insufficient (35.8% and 38.8%), and
substantive-other (2.5% and 3.3%).

Figure <a href="#fig:lsc-classification-frame-balance"
data-reference-type="ref"
data-reference="fig:lsc-classification-frame-balance">5.2</a> shows a
shift toward lived-experience framing, especially in ADHD discourse. In
the full predicted frame composition (Panel A), lived-experience-only
assignments increased from 10.7% to 15.7% of ADHD contexts between
2014–2017 and 2022–2026, while clinical-only assignments fell from 44.4%
to 39.1%. Autism showed the same direction more weakly:
lived-experience-only assignments increased from 17.6% to 19.6%, while
clinical-only assignments fell from 31.3% to 27.7%. The annual trend
model then restricts the denominator to the clear clinical-only and
lived-experience-only assignments, estimating the lived-experience share
among those two frames (Panel B). On this contrast, ADHD increased by
1.07 percentage points per year ($`p<.001`$), a fitted 12.8-point rise
from 2014 to 2026. Autism increased by 0.59 points per year
($`p=.013`$), a fitted 7.0-point rise, although this series was flagged
for residual autocorrelation and the AR(1) sensitivity slope did not
differ from zero ($`p=.370`$). The trend model table is reported in
Appendix Table <a href="#tab:lsc-classification-frame-time-trends"
data-reference-type="ref"
data-reference="tab:lsc-classification-frame-time-trends">8.1</a>.

<figure id="fig:lsc-classification-frame-balance"
data-latex-placement="!htbp">
<embed src="lsc/classification/lsc_classification_frame_balance.pdf" />
<figcaption>Predicted frame composition and clear-frame lived-experience
trajectories for ADHD and autism target contexts. Panel A retains all
predicted derived-frame labels and compares early (2014–2017) with late
(2022–2026) periods. Panel B shows annual lived-experience share among
contexts assigned either clinical-only or lived-experience-only; shaded
bands are 95% document-bootstrap intervals and dashed lines are OLS
trend summaries over publication years.</figcaption>
</figure>

## Sentiment

Sentiment estimates were based on 17,649 ADHD and 39,463 autism contexts
in the overall aggregate. At least one NRC–VAD match occurred in 17,255
ADHD contexts (97.8%) and 39,200 autism contexts (99.3%), yielding
58,666 and 144,668 matched collocate occurrences, respectively. Within
individual years, at least 96.2% of ADHD contexts and 98.9% of autism
contexts contributed a match, while matched-token coverage was at least
88.7% and 87.6%, respectively. The resulting annual valence index ranges
from $`-1`$ to $`1`$, with higher values indicating more positive local
wording.

##### ADHD.

Annual overall valence ranged from .044 to .106 ($`M=.076`$,
$`SD=.020`$). Its positive linear slope did not differ from zero
($`B=0.0023`$, $`p=.113`$, adjusted $`R^2=.14`$); neither did the
clinical or lived-experience slope. The overall and clinical residuals
were flagged for autocorrelation, and the AR(1) sensitivity slopes also
did not differ from zero. Quadratic diagnostics fitted U-shaped overall
and clinical trajectories, with minima in August 2018 and June 2019 and
adjusted-$`R^2`$ gains of .29 and .60 over the corresponding linear
models.

##### Autism.

Annual overall valence ranged from .137 to .173 ($`M=.149`$,
$`SD=.009`$). The overall slope ($`B=0.0012`$, $`p=.081`$, adjusted
$`R^2=.18`$) and the clinical and lived-experience OLS slopes did not
differ from zero. The clinical residuals were flagged for
autocorrelation; the AR(1) sensitivity slope was positive ($`B=0.0052`$,
$`p=.040`$). The quadratic model fitted a U-shaped clinical trajectory
with a minimum in February 2019 and an adjusted-$`R^2`$ gain of .65.

Post-hoc contributor patterns indicate that positive valence
contributions were concentrated around child- and family-related
vocabulary, while lower-valence contributions came mainly from
diagnostic, disability, and co-occurring-condition terms, including
*autism* in ADHD contexts and *ADHD* in autism contexts (Appendix
Table <a href="#tab:lsc-posthoc-sentiment-contributors"
data-reference-type="ref"
data-reference="tab:lsc-posthoc-sentiment-contributors">8.5</a>).

Figure <a href="#fig:lsc-sentiment-diagnostics" data-reference-type="ref"
data-reference="fig:lsc-sentiment-diagnostics">5.3</a> presents the
flagged sentiment trajectories with linear and quadratic fits.
Coefficients for the quadratic models are reported in Appendix
Table <a href="#tab:lsc-quadratic-diagnostics" data-reference-type="ref"
data-reference="tab:lsc-quadratic-diagnostics">8.4</a>.

<figure id="fig:lsc-sentiment-diagnostics" data-latex-placement="!htbp">
<embed
src="lsc/diagnostics/lsc_quadratic_diagnostics_sentiment_targets.pdf" />
<figcaption>Annual mean valence for the flagged ADHD overall, ADHD
clinical, and autism clinical trajectories, with 95% document-bootstrap
intervals and linear and quadratic fits. Autism overall estimates and
intervals are shown as muted contextual reference.</figcaption>
</figure>

## Intensity

Intensity uses the same NRC–VAD matches as sentiment and is therefore
based on the same 17,649 ADHD and 39,463 autism contexts and 58,666 and
144,668 matched collocate occurrences. The context- and token-coverage
rates reported above also apply to the annual arousal estimates. The
arousal index ranges from $`-1`$ to $`1`$, with higher values indicating
greater emotional activation.

##### ADHD.

Annual overall arousal ranged from .011 to .032 ($`M=.023`$,
$`SD=.007`$). Contrary to hypothesis the fitted slope did not differ
from zero overall ($`B=0.0001`$, $`p=.900`$, adjusted $`R^2=-.09`$), in
clinical contexts ($`B=0.0002`$, $`p=.738`$, adjusted $`R^2=-.08`$), or
in lived-experience contexts ($`B=0.0015`$, $`p=.123`$, adjusted
$`R^2=.13`$).

##### Autism.

Annual overall arousal ranged from approximately .000 to .047
($`M=.010`$, $`SD=.012`$). The overall slope was positive but did not
differ from zero ($`B=0.0011`$, $`p=.223`$, adjusted $`R^2=.05`$),
contrary to expectation. Clinical arousal increased nominally
($`B=0.0010`$, $`p=.048`$, adjusted $`R^2=.25`$), while the
lived-experience slope did not differ from zero ($`B=0.0025`$,
$`p=.252`$, adjusted $`R^2=.04`$).

Post-hoc arousal contributor patterns were concentrated in diagnostic
and difficulty-related vocabulary. ADHD arousal was raised most
consistently by terms such as *disorder*, *anxiety*, *hyperactivity*,
*challenge*, and *struggle*; autism arousal was raised by *disorder*,
*developmental*, *spectrum*, and, in later lived-experience contexts,
terms such as *dangerous* and *obsession*. Lower-arousal contributors
included treatment-, individual-, people-, and family-related vocabulary
(Appendix Table <a href="#tab:lsc-posthoc-arousal-contributors"
data-reference-type="ref"
data-reference="tab:lsc-posthoc-arousal-contributors">8.6</a>).
Figure <a href="#fig:lsc-intensity" data-reference-type="ref"
data-reference="fig:lsc-intensity">5.4</a> presents the target and
comparator trajectories. Among the comparators, sadness increased; the
OLS slopes for frustration and loneliness did not differ from zero.

<figure id="fig:lsc-intensity" data-latex-placement="!htbp">
<embed src="lsc/intensity/lsc_intensity_arousal_trajectories.pdf" />
<figcaption>Annual mean NRC–VAD arousal for ADHD, autism, and comparator
contexts. Shaded bands are 95% document-bootstrap intervals; dashed
lines are OLS trend summaries.</figcaption>
</figure>

## Breadth

Breadth estimates were available for 57,112 target contexts in the three
substantive frames. After within-cell exact-duplicate removal, 53,544
contexts contributed to the frame-specific estimates; the same material
was also aggregated into the overall target series, yielding 107,081
target analysis rows. The comparator sample contributed 38,349 rows. The
annual index is the mean pairwise cosine distance, with higher values
indicating greater contextual dispersion.

##### ADHD.

Breadth was .138 in 2014 and .117 in 2026 overall ($`M=.127`$,
$`SD=.009`$). Contrary to hypothesis, it decreased overall
($`B=-0.0020`$, $`p<.001`$, adjusted $`R^2=.77`$) and in clinical
contexts ($`B=-0.0018`$, $`p<.001`$, adjusted $`R^2=.69`$). The
lived-experience slope did not differ from zero ($`B=-0.0004`$,
$`p=.560`$, adjusted $`R^2=-.06`$) and its residuals were flagged for
autocorrelation.
Figure <a href="#fig:lsc-breadth" data-reference-type="ref"
data-reference="fig:lsc-breadth">5.5</a> presents these trajectories
alongside the autism and comparator series.

<figure id="fig:lsc-breadth" data-latex-placement="!htbp">
<embed src="lsc/breadth/lsc_breadth_trajectories.pdf" />
<figcaption>Annual mean pairwise cosine distance among target-aware
contextual embeddings for ADHD, autism, and comparator terms. Higher
values indicate greater contextual breadth; shaded bands are 95%
document-bootstrap intervals.</figcaption>
</figure>

##### Autism.

Breadth was .096 in 2014 and .098 in 2026 overall ($`M=.099`$,
$`SD=.003`$). Inconsistent with expectation, its linear slope did not
differ from zero ($`B=0.0001`$, $`p=.637`$, adjusted $`R^2=-.07`$), and
the flagged AR(1) sensitivity slope also did not differ from zero
($`p=.753`$). Lived-experience breadth decreased ($`B=-0.0010`$,
$`p=.030`$, adjusted $`R^2=.30`$). The clinical OLS slope was nominally
positive ($`B=0.0007`$, $`p=.036`$, adjusted $`R^2=.28`$), but its
residuals were flagged for autocorrelation and the AR(1) sensitivity
slope did not differ from zero ($`p=.755`$).

The breadth diagnostic suggests different lexical profiles among the
highest-distance sampled contexts. ADHD high-distance contexts
repeatedly surfaced diagnostic, educational, and co-occurring-condition
vocabulary, including *disorder*, *impulsivity*, *inattention*,
*learning*, *dyslexia*, and *autism*. Autism high-distance contexts were
more varied: clinical contexts included terms such as *research*,
*organization*, *developmental*, and *treatment*, while overall and
lived-experience contexts included broader contextual words such as
*world*, *feel*, *people*, and *experience* (Appendix
Table <a href="#tab:lsc-posthoc-breadth-contributors"
data-reference-type="ref"
data-reference="tab:lsc-posthoc-breadth-contributors">8.7</a>).

Figure <a href="#fig:lsc-breadth-autism-diagnostics" data-reference-type="ref"
data-reference="fig:lsc-breadth-autism-diagnostics">5.6</a> presents
quadratic fit diagnostics for the two flagged autism series. The overall
trajectory had an inverted-U fit with a vertex in April 2020, while the
clinical fit had a vertex in May 2021. The adjusted-$`R^2`$ gains over
the corresponding linear models were .58 and .51. Appendix
Table <a href="#tab:lsc-quadratic-diagnostics" data-reference-type="ref"
data-reference="tab:lsc-quadratic-diagnostics">8.4</a> reports the model
coefficients.

<figure id="fig:lsc-breadth-autism-diagnostics"
data-latex-placement="!htbp">
<embed
src="lsc/diagnostics/lsc_quadratic_diagnostics_breadth_autism.pdf"
style="width:80.0%" />
<figcaption>Annual overall and clinical autism breadth, with 95%
document-bootstrap intervals and linear and quadratic fits. Both series
were selected following residual-autocorrelation flags.</figcaption>
</figure>

## Neighbour Similarity Evolution

The thematic analysis began with 57,112 tokenised target contexts, of
which 56,857 remained after content filtering. The complete annual
top-five output contained 390 neighbour rows across two targets, three
reporting strata, and 13 years. Stability filtering retained 28
neighbours and 364 annual similarity estimates.

For ADHD, the stable overall neighbours were *child*, *disorder*,
*symptom*, *diagnose*, and *condition*. The clinical set contained the
same diagnostic and child-related vocabulary, while the lived-experience
set retained *help*, *child*, and *diagnose*. These patterns are
consistent with the corresponding frame definitions. The ADHD
trajectories are reported in Appendix
Figure <a href="#fig:lsc-thematic-adhd-appendix" data-reference-type="ref"
data-reference="fig:lsc-thematic-adhd-appendix">8.1</a>.

Figure <a href="#fig:lsc-thematic-autism" data-reference-type="ref"
data-reference="fig:lsc-thematic-autism">5.7</a> presents the autism
trajectories. *Child* remained the highest-similarity stable neighbour
overall and in clinical contexts. The remaining clinical neighbours were
*disorder*, *research*, *study*, and *developmental*, while the
lived-experience set comprised *people*, *child*, *support*, *life*, and
*family*. Both sets are consistent with the respective frame
definitions.

<figure id="fig:lsc-thematic-autism" data-latex-placement="!htbp">
<embed
src="lsc/thematic_evolution/lsc_thematic_neighbour_similarity_autism.pdf" />
<figcaption>Annual cosine similarity between the autism concept token
and stable Word2Vec neighbours in overall, clinical/disorder, and
lived-experience contexts. Stable neighbours appeared in an annual
top-five list in at least two years and had estimates in at least ten
years.</figcaption>
</figure>

## Robustness Checks

The robustness checks compared the main affective and breadth measures
with the original SIBling operationalisations: Warriner affect norms for
sentiment and intensity and MPNet sentence embeddings for breadth . The
annual strata and trend-model convention were held constant.

Under Warriner norms, the ADHD sentiment estimate remained positive but
did not differ from zero ($`B=0.0003`$, $`p=.366`$, adjusted
$`R^2=-.01`$). The autism sentiment estimate was positive ($`B=0.0005`$,
$`p=.018`$, adjusted $`R^2=.36`$), and its residuals were flagged for
autocorrelation; the AR(1) sensitivity estimate also differed from zero
($`B=0.0005`$, $`p<.001`$).

Intensity was more sensitive to the affective resource. The near-zero
NRC–VAD ADHD slope ($`B=0.0001`$, $`p=.900`$, adjusted $`R^2=-.09`$)
became negative under Warriner ($`B=-0.0009`$, $`p<.001`$, adjusted
$`R^2=.62`$), and the AR(1) sensitivity estimate remained negative
($`B=-0.0011`$, $`p=.008`$). For autism, the NRC–VAD overall slope was
positive but did not differ from zero ($`B=0.0011`$, $`p=.223`$,
adjusted $`R^2=.05`$), while the Warriner estimate was negative and also
did not differ from zero ($`B=-0.0002`$, $`p=.641`$, adjusted
$`R^2=-.07`$).

The breadth trajectories also differed by encoder. ADHD breadth declined
under XL-LEXEME ($`B=-0.0020`$, $`p<.001`$, adjusted $`R^2=.77`$), while
the MPNet slope was weaker and did not differ from zero ($`B=-0.0004`$,
$`p=.445`$, adjusted $`R^2=-.03`$). Autism changed from the non-linear,
approximately flat overall XL-LEXEME trajectory ($`B=0.0001`$,
$`p=.637`$, adjusted $`R^2=-.07`$) to a negative MPNet slope
($`B=-0.0014`$, $`p=.001`$, adjusted $`R^2=.59`$).
Figure <a href="#fig:lsc-method-robustness" data-reference-type="ref"
data-reference="fig:lsc-method-robustness">5.8</a> shows the intensity
and breadth comparisons as change from each method’s 2014 estimate.

<figure id="fig:lsc-method-robustness" data-latex-placement="!htbp">
<embed
src="lsc/robustness/lsc_method_robustness_intensity_breadth.pdf" />
<figcaption>Method sensitivity of overall target intensity and breadth
trajectories, expressed as change from each method’s 2014 estimate. Main
measures use NRC–VAD and XL-LEXEME; the original SIBling
operationalisations use Warriner norms and MPNet. Shaded bands are
shifted annual bootstrap intervals.</figcaption>
</figure>

# Discussion

This study asked whether ADHD and autism—two diagnostic concepts at the
centre of contemporary debates about the popularisation of mental health
language—have undergone the semantic loosening that concept creep theory
predicts, and it did so in general web discourse rather than in the
curated corpora on which prior evidence largely rests. The answer, on
the measures used here, is that they have not. Across a frame-aware
adaptation of the SIBling framework , the dominant empirical signature
was not milder, broader, or diluted meaning, but a reorganisation of the
discourse surrounding two comparatively stable concepts. This chapter
first summarises the findings in relation to the research questions
(Section <a href="#sec:discussion-summary" data-reference-type="ref"
data-reference="sec:discussion-summary">6.1</a>). It then develops three
interpretive claims: that the results indicate semantic consolidation
rather than concept creep
(Section <a href="#sec:discussion-semantic-consolidation"
data-reference-type="ref"
data-reference="sec:discussion-semantic-consolidation">6.2</a>); that
the clearest change in ADHD and autism discourse over the period was
compositional—a shift from disorder talk toward lived experience,
accompanied by a recent positification (a positive valence shift) of
clinical contexts
(Section <a href="#sec:discussion-lived-experience" data-reference-type="ref"
data-reference="sec:discussion-lived-experience">6.3</a>); and that the
robustness checks expose a measurement sensitivity with implications for
the wider literature on lexical semantic change
(Section <a href="#sec:discussion-measurement-sensitivity"
data-reference-type="ref"
data-reference="sec:discussion-measurement-sensitivity">6.4</a>).
Limitations
(Section <a href="#sec:discussion-limitations" data-reference-type="ref"
data-reference="sec:discussion-limitations">6.5</a>) and directions for
future research
(Section <a href="#sec:discussion-future-research" data-reference-type="ref"
data-reference="sec:discussion-future-research">6.6</a>) close the
chapter.

## Summary of Findings

With respect to RQ1, ADHD and autism did not become jointly more
prominent in sampled general web discourse between 2014 and 2026. Autism
salience declined by a fitted 28.9%, while the fitted 27.0% increase for
ADHD did not differ from zero at the uncorrected $`p < .05`$ level. The
comparators moved in different directions—loneliness rose, sadness fell,
and frustration showed no reliable trend—so the autism decline cannot be
attributed to a uniform contraction of negative-affect language in the
crawl.

With respect to RQ2, the balance of framing shifted toward lived
experience. Among contexts assigned to a single clear frame, the
lived-experience share of ADHD discourse rose by 1.07 percentage points
per year, a fitted increase from 17.5% to 30.3%; the corresponding
autism trend was directionally similar but roughly half the size and was
not retained by the AR(1) sensitivity model. The full predicted
composition showed the same pattern, with clinical-only shares falling
for both targets.

With respect to RQ3, neither of the hypothesised forms of concept creep
was supported
(Table <a href="#tab:lsc-regression-target-frames" data-reference-type="ref"
data-reference="tab:lsc-regression-target-frames">5.1</a>). Contrary to
the vertical-creep hypothesis, intensity did not decline in any target
stratum; the only slope that differed from zero was a nominal increase
in autism clinical contexts. Contrary to the horizontal-creep
hypothesis, breadth decreased for ADHD overall and in ADHD clinical
contexts—the strongest semantic trends observed in the study—decreased
in autism lived-experience contexts, and was flat for autism overall,
with the nominally positive autism clinical slope not surviving the
AR(1) check. Sentiment showed no linear trend in any target stratum;
instead, the autocorrelation-flagged series traced U-shaped trajectories
with minima between mid-2018 and mid-2019, and the AR(1) sensitivity
model indicated a positive valence trend in autism clinical contexts.

With respect to RQ4, the thematic content of both discourses changed
remarkably little. The 28 neighbours retained by the stability filter
were diagnostic and child- or family-centred throughout: *child*,
*disorder*, *symptom*, *diagnose*, and *condition* anchored ADHD
discourse, while *child* remained the highest-similarity neighbour of
autism overall and in clinical contexts, with *people*, *support*,
*life*, and *family* characterising lived-experience contexts.

Semantic Consolidation Rather than Concept\
Creep
-------------------------------------------

The central substantive finding is a double null with a directional
surprise. ADHD and autism did not drift into less emotionally intense
contexts, and they did not disperse across a broader range of lexical
contexts; where reliable movement occurred, it ran in the opposite
direction, most clearly in the sustained contraction of ADHD breadth
from .138 to .117. This pattern is unlikely to reflect measures that
were simply too blunt to register change of any kind: the same
instruments detected reliable trends among the comparators, including a
broadening of frustration, an intensification of sadness contexts, and
robust positification of frustration and loneliness. Within this corpus
and this measurement scheme, the affective and contextual profiles of
ADHD and autism were among the more stable series observed.

This stability is intelligible in light of ’s finding that
long-established, codified psychological terms such as *OCD* and
*bipolar* showed little year-to-year semantic movement on Reddit,
whereas newer vernacular terms such as *gaslighting* shifted
substantially. ADHD and autism are institutionally anchored concepts:
their reference is stabilised by diagnostic manuals, clinical
gatekeeping, service categories, and educational law in ways that
ordinary-language harm concepts such as *bullying* or *harassment* are
not. Concept creep in such categories may therefore operate primarily at
the level of diagnostic criteria and category application—who is counted
as an instance—rather than at the level of the contexts in which the
label is used. This reading is consistent with ’s meta-analytic evidence
that ADHD’s diagnostic criteria inflated substantially across successive
DSM revisions, and it suggests a distinction that the concept creep
literature has tended to elide: a category can expand extensionally,
absorbing many more people, while the typical linguistic contexts of its
name remain stable or even converge. Collocate- and embedding-based
measures index the latter, not the former. On this account, the rising
prevalence documented in the epidemiological literature need not leave
the semantic trace that a naive application of concept creep theory
would predict.

The ADHD breadth contraction invites a further interpretation:
consolidation of a discourse genre. As ADHD moved from a specialist
topic to a mainstream one, its web coverage appears to have converged on
a recognisable register—symptom lists, awareness explainers, parenting
and educational advice—rather than diversifying. The post-hoc
diagnostics support this reading: the highest-distance ADHD contexts
remained saturated with diagnostic and educational vocabulary
(*disorder*, *impulsivity*, *inattention*, *learning*, *dyslexia*), and
the stable neighbours were uniformly diagnostic and child-related.
Alternatively, the contraction may reflect the composition of the sample
rather than the concept: if health articles written to a common template
(e.g., for search-engine optimisation) account for a growing share of
retained ADHD documents, their near-identical contexts would pull
measured breadth down even if the meaning of ADHD itself were unchanged.
The two possibilities are compatible and cannot be separated here.
Either way, the frustration comparator’s concurrent broadening indicates
that the contraction is target-specific rather than a corpus-wide
artefact.

The findings also bear on the evidentiary question that motivated the
study. Prior evidence for creep in mental-health-related concepts
derives largely from psychology abstracts and curated general-language
corpora , and have shown that apparent semantic trends in such corpora
can be artefacts of shifting discourse composition. The present study
addressed both concerns—moving to general web discourse and stratifying
by frame—and found no creep for two concepts whose diagnostic categories
have demonstrably expanded. This does not overturn earlier findings for
other concepts, which concerned different terms with different
institutional standing, but it does reinforce the conclusion that creep
results are corpus- and composition-dependent, and that inferences about
cultural change drawn from any single curated corpus should be made
cautiously. At the same time, Common Crawl imposes structural
constraints of its own , a point developed in
Section <a href="#sec:discussion-limitations" data-reference-type="ref"
data-reference="sec:discussion-limitations">6.5</a>; the discrepancy
between corpora identifies corpus dependence rather than settling which
corpus better reflects the underlying culture.

The salience results sharpen this point. Autism’s declining prominence
in the sampled crawl sits awkwardly beside steeply rising diagnosed
prevalence and the concept’s conspicuous platform presence, with more
than four million TikTok videos hashtagged by June 2026. The most
plausible reconciliation is that this discourse growth is concentrated
on social platforms and, increasingly, behind robots.txt
restrictions—both largely outside Common Crawl’s reach —while the open
web sampled here reflects a different, more institutional discourse
environment. Salience is also indexed by source year rather than
publication year, and crawl policy itself changed over the window. The
finding should therefore be read less as evidence that public interest
in autism waned and more as a caution: “web discourse” is not a single
population, and the discourse boom visible on TikTok is not
automatically visible—or even present—in web-scale archives.

## From Disorder Talk to Lived Experience

If the meanings of ADHD and autism held broadly steady, the way they are
talked about did not. The most reliable change detected in the entire
study concerns who and what these terms are used to discuss: ADHD
discourse shifted markedly from clinical toward lived-experience
framing, and autism discourse moved in the same direction from a
substantially higher starting point (a fitted 35.9% lived-experience
share in 2014, against 17.5% for ADHD). This asymmetry is theoretically
coherent. Identity-first and community framings of autism were
established well before the study window, whereas ADHD’s popular
turn—adult self-recognition, online community formation, and
identity-oriented content—is more recent, consistent with qualitative
evidence on ADHD online communities , clinical reports of self-diagnosed
presentations , the sharp growth in ADHD media coverage , and accounts
of “platformed diagnosis” spilling outward from social media . On this
reading, ADHD is undergoing in the 2020s a reframing that autism
discourse partially completed earlier, leaving autism’s trend flatter
and, after accounting for serial dependence, statistically unresolved.

The compositional shift also vindicates the study’s frame-aware design
on its own terms. Frame-specific trajectories repeatedly diverged in
sign: autism clinical breadth trended nominally upward while
lived-experience breadth declined; autism clinical sentiment was
positive under the AR(1) model while the lived-experience slope pointed
downward. An unstratified analysis would have blended these movements
with the changing frame mixture, precisely the conflation that
identified in newspaper text. The present results extend that
demonstration to web-scale data and to a different kind of composition
problem. For anxiety and depression, stratification must first separate
clinical constructs from the ordinary states and non-clinical phenomena
(e.g., everyday anxiety, or an economic depression) the same words
denote ; ADHD and autism are diagnostic concepts by definition, so the
composition that shifts is the framing of the diagnosis itself—as
disorder construct or as identity and lived experience.

The sentiment results add a temporal texture that linear models missed.
The flagged ADHD and autism clinical series traced U-shapes with minima
between August 2018 and June 2019, implying declining valence through
the mid-2010s followed by recovery through 2026, with the autism
clinical positification robust to first-order autocorrelation. The
turning point falls within the period in which neurodiversity entered
mainstream media vocabulary, with US news coverage of neurodiversity and
neurodivergent individuals increasing between 2016 and 2022 , and in
which first-person ADHD and autism content proliferated on social
platforms . The post-hoc contributors indicate that the recovery is
carried by child-, family-, and support-related vocabulary against a
persistent background of diagnostic and comorbidity terms. Even so, this
is at most gentle destigmatisation, and it is not uniform: the
appearance of *dangerous* and *obsession* among the late-period
arousal-raising collocates in autism lived-experience contexts shows
that alarmed or stigmatising discourse persists.

A further pattern deserves note. From 2018 onward, *adhd* entered the
leading negative-valence contributors in autism contexts, mirroring the
persistent presence of *autism* in ADHD contexts, and 11,795 corpus
documents contained both targets. This is web-general corroboration of
the ADHD–autism semantic convergence that reported on Reddit from 2019:
the two concepts are increasingly discussed together, as co-occurring
conditions and as jointly constitutive of neurodivergent identity. The
convergence also complicates target-specific measurement, since each
concept increasingly forms part of the other’s context.

Taken together, these results speak to the therapy-speak debate with
which this project opened. The trivialisation concern holds that
popularisation erodes the meaning of clinical terms, dispersing them
across ever milder and more heterogeneous uses and thereby depriving
diagnosed people of precise language for their experiences . For ADHD
and autism in general web discourse, the semantic preconditions of that
concern are not in evidence: contexts did not become milder, did not
broaden, and remained thematically anchored to diagnosis, childhood, and
family. A different hermeneutical concern, however, does find support:
despite the steep rise in adult diagnoses , the stable thematic core of
both discourses remained resolutely child-centred, suggesting that
late-diagnosed adults continued to encounter a web discourse organised
around children’s presentations rather than their own.

Measurement Sensitivity and Its Implications for\
LSC Research
-------------------------------------------------

The robustness checks carry an implication that extends beyond this
study. Substituting the original SIBling resources for the refined ones
changed some substantive conclusions. Under Warriner norms, ADHD
intensity declined reliably, a result that, taken alone, would have been
reported as vertical concept creep—whereas the NRC–VAD estimate was
flat. Under MPNet sentence embeddings, the pronounced XL-LEXEME decline
in ADHD breadth attenuated to a null, while autism’s flat XL-LEXEME
trajectory became a reliable MPNet decline. Two conclusions were common
to both operationalisations and can be stated with corresponding
confidence: neither target broadened horizontally, and neither showed a
reliable linear sentiment trend. The vertical-creep conclusion, by
contrast, is measurement-conditional and should be reported as such.

There are principled grounds for preferring the refined measures a
priori: NRC–VAD offers roughly threefold lexical coverage, multi-word
entries, and higher reported reliability , and XL-LEXEME is a
word-in-context model built for LSC detection that represents the target
use itself rather than the surrounding sentence topic , a distinction
that matters when the analytic object is a term’s usage rather than a
document’s subject matter. But the a priori argument does not dissolve
the empirical problem: the SIBling dimensions, as currently
operationalised, are not measurement-invariant, a possibility its
authors anticipated in describing the operationalisations as first
implementations . This observation may also help explain the mixed
findings accumulating in the literature—increases in semantic intensity
where creep predicts decreases , reversals across corpora , and
composition-dependent trends —some portion of which plausibly reflects
instrumentation rather than substance. Until the framework’s dimensions
are validated against human judgements of intensity, breadth, and
connotation, LSC studies in this tradition would do well to treat
multi-resource robustness checks not as optional supplements but as part
of the primary analysis, as practised here.

## Limitations

The most consequential limitations concern the corpus. Common Crawl is
neither a complete copy of the web nor a representative sample of it:
domain selection follows harmonic centrality, adopted in 2017 (prior
crawls relied mainly on externally donated URL seed lists, which had
dwindled over time); major social platforms are absent; and
rights-holder blocking via robots.txt has grown over precisely the years
studied . Temporal comparability is therefore imperfect, and
trends—salience above all—partly reflect changes in what the crawl could
see. Findings generalise to quality-filtered, English-language open-web
discourse as archived by Common Crawl, not to public discourse at large,
and emphatically not to the social platforms where much contemporary
ADHD and autism discourse occurs.

The frame-aware design inherits classifier error. The lived-experience
head performed adequately but weakest (F1 = .760, precision = .704), so
lived-experience strata contain clinical admixture, which will have
attenuated frame contrasts and blurred frame-specific trends. The human
gold standard also rests on a single coder: the pilot, the adjudication
of flagged cases, and the 200-passage validation set reflect one
annotator’s judgement, so no inter-annotator agreement can be reported
and validation performance is measured against an individual rather than
a consensus standard. And although the ACT workflow retained human
authority over labels, LLM-assisted annotation may import model-specific
biases that human review of high-risk cases only partially corrects. The
frame codebook, finally, is specific to ADHD and autism, so the
baselines could not be frame-stratified, limiting target–baseline
comparisons to unframed series.

Three further limitations should be explicit. The affective measures
score lemmatised collocates with context-free lexicon values within the
$`\pm`$<!-- -->5-token windows adopted from the SIBling
operationalisation , and are therefore blind to negation, irony, and
syntactic scope. The comparators are baselines for general
negative-affect language, not matched semantic controls. And the design
as a whole is observational: it can characterise how discourse changed,
but not why.

## Directions for Future Research

Three extensions follow directly. First, cross-platform designs are
needed to test the suggestion advanced in
Section <a href="#sec:discussion-semantic-consolidation"
data-reference-type="ref"
data-reference="sec:discussion-semantic-consolidation">6.2</a> that ADHD
and autism discourse has migrated toward social platforms outside Common
Crawl’s reach: applying the same frame-aware measures to web, Reddit,
and short-video corpora over a common window would show whether the
semantic stability observed here coexists with creep in platform
discourse . Second, the measurement sensitivity identified in
Section <a href="#sec:discussion-measurement-sensitivity"
data-reference-type="ref"
data-reference="sec:discussion-measurement-sensitivity">6.4</a>
motivates validation studies in which lexicon- and embedding-based
intensity, breadth, and sentiment estimates are benchmarked against
human judgements of the same contexts, so that resource choice can rest
on demonstrated convergent validity rather than plausibility arguments.
Third, the substantive account developed here generates testable
expectations: if institutional codification protects diagnostic labels
from creep, then frame-aware analyses of less codified neighbouring
vocabulary—*neurodivergent*, *masking*, *stimming*, *executive
dysfunction*—should reveal greater semantic movement than ADHD and
autism themselves; and linking annual discourse measures to diagnostic
and prescribing statistics would allow the temporal ordering assumed by
the prevalence inflation hypothesis to be examined directly.
Finer-than-annual time resolution, non-English web discourse, and
qualitative analysis of the contentious late-period lived-experience
contexts flagged in
Section <a href="#sec:discussion-lived-experience" data-reference-type="ref"
data-reference="sec:discussion-lived-experience">6.3</a> would each add
further resolution to the picture drawn here.

# Conclusion

This research project examined lexical semantic change in ADHD and
autism across thirteen years of general web discourse, motivated by the
concern that the popularisation of mental health language erodes the
meaning of the terms on which diagnosed people rely. It constructed a
reproducible Common Crawl pipeline spanning 2014–2026, separated
clinical from lived-experience framing with a validated hierarchical
classifier, and estimated salience, sentiment, intensity, breadth, and
thematic-neighbour trajectories using a refined adaptation of the
SIBling framework, benchmarked against the framework’s original
operationalisations.

The hypothesised concept creep did not materialise. ADHD and autism did
not drift into milder emotional contexts, and their contexts did not
broaden; ADHD contexts contracted markedly, and the thematic core of
both discourses—diagnosis, childhood, family—remained stable throughout.
What changed was the composition of the discourse: a robust shift from
clinical toward lived-experience framing, strongest for ADHD, together
with a recent, tentative positification of clinical contexts and growing
entanglement between the two concepts. In sampled open-web discourse,
autism became less prominent rather than more, underscoring that the
contemporary surge in ADHD and autism talk is unevenly distributed
across the digital landscape.

The study makes four contributions. Substantively, it extends concept
creep and therapy-speak research to ADHD and autism and provides
evidence that two institutionally codified diagnostic concepts resisted
the semantic loosening documented for ordinary-language harm concepts,
with category expansion proceeding through application rather than
through the dilution of meaning in use. Evidentially, it moves LSC
research on mental-health concepts from curated corpora to general web
discourse and shows that conclusions do not simply transfer between the
two. Analytically, it demonstrates at web scale that discourse
composition and semantic change must be separated, since frame-specific
trajectories repeatedly diverged from aggregates. Methodologically, its
robustness checks show that key SIBling conclusions are conditional on
lexicon and encoder choice, identifying measurement validation as a
priority for the field.

The broader implication is a reframing of the trivialisation concern.
For ADHD and autism on the open web, the words have largely held their
shape; it is the conversation around them—who speaks, in what frame, and
on which platforms—that has been transformed. Whether the same holds
where that conversation is now loudest remains the question this project
leaves open, and the tools it provides are designed to answer it.

# Appendices

## Frame Classification Trend Models

Table <a href="#tab:lsc-classification-frame-time-trends"
data-reference-type="ref"
data-reference="tab:lsc-classification-frame-time-trends">8.1</a>
reports the descriptive trend models behind the clear-frame
lived-experience trajectories in
Figure <a href="#fig:lsc-classification-frame-balance"
data-reference-type="ref"
data-reference="fig:lsc-classification-frame-balance">5.2</a>.

<div id="tab:lsc-classification-frame-time-trends">

| Target | $`B(SE)`$ | $`\beta`$ | Adj. $`R^2`$ | Fitted 2014 | Fitted 2026 |
|:---|:--:|:--:|:--:|:--:|:--:|
| ADHD | $`1.07^{***}`$ (0.15) | 0.90 | 0.80 | 17.5% | 30.3% |
| Autism | $`0.59^{*\dagger}`$ (0.20) | 0.67 | 0.39 | 35.9% | 42.9% |

Descriptive annual trend models for clear-frame lived-experience share.

</div>

Note. The outcome is annual lived-experience share among contexts
assigned to one clear substantive frame, defined as lived-only divided
by lived-only plus clinical-only. Years are document publication years.
$`B(SE)`$ is the annual OLS slope in percentage points per year;
$`\beta`$ is the standardised year coefficient. Significance markers:
$`^{*}p<.05`$, $`^{**}p<.01`$, $`^{***}p<.001`$. $`^{\dagger}`$
indicates a residual-autocorrelation flag; AR(1) sensitivity estimates
for flagged rows are reported in Appendix
Table <a href="#tab:lsc-ar1-sensitivity-flagged" data-reference-type="ref"
data-reference="tab:lsc-ar1-sensitivity-flagged">8.3</a>. Figure
intervals are 95% document-bootstrap intervals. P values are descriptive
and uncorrected.

## Baseline Comparator Trend Models

Table <a href="#tab:lsc-regression-baseline-comparators"
data-reference-type="ref"
data-reference="tab:lsc-regression-baseline-comparators">8.2</a> reports
the unframed comparator-term trend models used to contextualise the ADHD
and autism target trajectories.

<div id="tab:lsc-regression-baseline-comparators">

| Measure | Comparator | $`B(SE)`$ | $`\beta`$ | Adj. $`R^2`$ |
|:---|:---|:--:|:--:|:--:|
| Salience | frustration | $`-0.0175^{\dagger}`$ (0.0167) | -0.30 | 0.01 |
| Salience | loneliness | $`0.0056^{*}`$ (0.0021) | 0.62 | 0.33 |
| Salience | sadness | $`-0.0171^{**}`$ (0.0039) | -0.80 | 0.61 |
| Sentiment | frustration | $`0.0037^{*\dagger}`$ (0.0013) | 0.67 | 0.39 |
| Sentiment | loneliness | $`0.0044^{**\dagger}`$ (0.0013) | 0.71 | 0.45 |
| Sentiment | sadness | $`-0.0030^{**}`$ (0.0007) | -0.79 | 0.59 |
| Intensity | frustration | $`-0.0004^{\dagger}`$ (0.0006) | -0.23 | -0.03 |
| Intensity | loneliness | $`-0.0006`$ (0.0006) | -0.30 | 0.01 |
| Intensity | sadness | $`0.0028^{***}`$ (0.0004) | 0.88 | 0.76 |
| Breadth | frustration | $`0.0020^{**}`$ (0.0006) | 0.72 | 0.48 |
| Breadth | loneliness | $`-0.0007`$ (0.0004) | -0.51 | 0.19 |
| Breadth | sadness | $`-0.0010^{**\dagger}`$ (0.0003) | -0.71 | 0.46 |

Descriptive annual trend models for baseline comparator trajectories.

</div>

Note. Cells report annual unstandardised OLS slopes as $`B(SE)`$;
$`\beta`$ is the standardised year coefficient. Comparator terms are
unframed baseline series. Salience uses Common Crawl source year and
semantic measures use document publication year. Significance markers:
$`^{*}p<.05`$, $`^{**}p<.01`$, $`^{***}p<.001`$. $`^{\dagger}`$
indicates a residual-autocorrelation flag; AR(1) sensitivity estimates
for flagged rows are reported in Appendix
Table <a href="#tab:lsc-ar1-sensitivity-flagged" data-reference-type="ref"
data-reference="tab:lsc-ar1-sensitivity-flagged">8.3</a>. P values are
descriptive and uncorrected.

## AR(1) Sensitivity Models

Table <a href="#tab:lsc-ar1-sensitivity-flagged" data-reference-type="ref"
data-reference="tab:lsc-ar1-sensitivity-flagged">8.3</a> reports AR(1)
sensitivity estimates for daggered rows in the trend tables.

<div id="tab:lsc-ar1-sensitivity-flagged">

| Source | Measure | Series | Frame | OLS $`B`$ | AR(1) $`B`$ | AR(1) $`p`$ |
|:---|:---|:---|:---|:--:|:--:|:--:|
| Table <a href="#tab:lsc-regression-target-frames" data-reference-type="ref"
data-reference="tab:lsc-regression-target-frames">5.1</a> | Sentiment | ADHD | Overall | $`0.0023`$ | $`0.0037`$ | $`.147`$ |
| Table <a href="#tab:lsc-regression-target-frames" data-reference-type="ref"
data-reference="tab:lsc-regression-target-frames">5.1</a> | Sentiment | ADHD | Clinical | $`0.0013`$ | $`0.0036`$ | $`.218`$ |
| Table <a href="#tab:lsc-regression-target-frames" data-reference-type="ref"
data-reference="tab:lsc-regression-target-frames">5.1</a> | Sentiment | Autism | Clinical | $`0.0022`$ | $`0.0052`$ | $`.040`$ |
| Table <a href="#tab:lsc-regression-target-frames" data-reference-type="ref"
data-reference="tab:lsc-regression-target-frames">5.1</a> | Breadth | ADHD | Lived experience | $`-0.0004`$ | $`-0.0004`$ | $`.247`$ |
| Table <a href="#tab:lsc-regression-target-frames" data-reference-type="ref"
data-reference="tab:lsc-regression-target-frames">5.1</a> | Breadth | Autism | Overall | $`0.0001`$ | $`-0.0001`$ | $`.753`$ |
| Table <a href="#tab:lsc-regression-target-frames" data-reference-type="ref"
data-reference="tab:lsc-regression-target-frames">5.1</a> | Breadth | Autism | Clinical | $`0.0007`$ | $`0.0002`$ | $`.755`$ |
| Table <a href="#tab:lsc-classification-frame-time-trends"
data-reference-type="ref"
data-reference="tab:lsc-classification-frame-time-trends">8.1</a> | Frame classification | Autism | Lived vs clinical | $`0.59`$ | $`0.23`$ | $`.370`$ |
| Table <a href="#tab:lsc-regression-baseline-comparators"
data-reference-type="ref"
data-reference="tab:lsc-regression-baseline-comparators">8.2</a> | Salience | frustration | Baseline | $`-0.0175`$ | $`0.0160`$ | $`.538`$ |
| Table <a href="#tab:lsc-regression-baseline-comparators"
data-reference-type="ref"
data-reference="tab:lsc-regression-baseline-comparators">8.2</a> | Sentiment | frustration | Baseline | $`0.0037`$ | $`0.0116`$ | $`.003`$ |
| Table <a href="#tab:lsc-regression-baseline-comparators"
data-reference-type="ref"
data-reference="tab:lsc-regression-baseline-comparators">8.2</a> | Sentiment | loneliness | Baseline | $`0.0044`$ | $`0.0070`$ | $`.009`$ |
| Table <a href="#tab:lsc-regression-baseline-comparators"
data-reference-type="ref"
data-reference="tab:lsc-regression-baseline-comparators">8.2</a> | Intensity | frustration | Baseline | $`-0.0004`$ | $`-0.0022`$ | $`.078`$ |
| Table <a href="#tab:lsc-regression-baseline-comparators"
data-reference-type="ref"
data-reference="tab:lsc-regression-baseline-comparators">8.2</a> | Breadth | sadness | Baseline | $`-0.0010`$ | $`-0.0008`$ | $`.131`$ |

AR(1) sensitivity models for autocorrelation-flagged annual series.

</div>

Note. Rows are the series marked with $`^{\dagger}`$ in the compact
regression tables. OLS $`B`$ repeats the primary annual slope; AR(1)
$`B`$ reports the first-order autoregressive sensitivity slope for the
same series. The frame-classification row is reported in percentage
points per year; all other rows use the original index units per year. P
values are descriptive and uncorrected.

## Quadratic Trend Models

Table <a href="#tab:lsc-quadratic-diagnostics" data-reference-type="ref"
data-reference="tab:lsc-quadratic-diagnostics">8.4</a> reports the
quadratic models for annual LSC trajectories whose linear residuals were
flagged for autocorrelation.

<div id="tab:lsc-quadratic-diagnostics">

| Measure | Target | Frame | Linear $`B(SE)`$ | Quadratic $`B_2(SE)`$ | $`\Delta`$ Adj. $`R^2`$ | Vertex |
|:---|:---|:---|:--:|:--:|:--:|:--:|
| Breadth | ADHD | Lived experience | $`-0.0004`$ (0.0006) | $`-0.0000`$ (0.0002) | -0.11 | outside window |
| Breadth | Autism | Overall | $`0.0001`$ (0.0002) | $`-0.0002^{**}`$ (0.0000) | 0.58 | April 2020 (inverted U) |
| Breadth | Autism | Clinical | $`0.0007^{*}`$ (0.0003) | $`-0.0003^{***}`$ (0.0001) | 0.51 | May 2021 (inverted U) |
| Sentiment | ADHD | Overall | $`0.0023`$ (0.0014) | $`0.0009^{*}`$ (0.0003) | 0.29 | August 2018 (U) |
| Sentiment | ADHD | Clinical | $`0.0013`$ (0.0015) | $`0.0012^{**}`$ (0.0003) | 0.60 | June 2019 (U) |
| Sentiment | Autism | Clinical | $`0.0022`$ (0.0015) | $`0.0013^{***}`$ (0.0002) | 0.65 | February 2019 (U) |

Supplementary quadratic models for autocorrelation-flagged LSC
trajectories.

</div>

Note. Rows are restricted to annual series whose linear-model residuals
were flagged for autocorrelation. Linear $`B`$ is the annual OLS slope
from the primary trend model. Quadratic $`B_2`$ is the
centred-year-squared coefficient; $`\Delta`$ Adj. $`R^2`$ is the
adjusted-$`R^2`$ gain over the linear model. Vertex labels give the
calendar month containing the fitted decimal-year vertex when it falls
inside the observed annual window; otherwise the vertex is marked as
outside the window. Significance markers are descriptive and
uncorrected: $`^{*}p<.05`$, $`^{**}p<.01`$, $`^{***}p<.001`$.

## Post-hoc Contributor Tables

Tables <a href="#tab:lsc-posthoc-sentiment-contributors"
data-reference-type="ref"
data-reference="tab:lsc-posthoc-sentiment-contributors">8.5</a>–<a href="#tab:lsc-posthoc-breadth-contributors"
data-reference-type="ref"
data-reference="tab:lsc-posthoc-breadth-contributors">8.7</a> summarise
the main VAD collocate contributors and the breadth diagnostic inspected
after the annual LSC analyses.

<div id="tab:lsc-posthoc-sentiment-contributors">

| Target | Frame            | 2014-2017 | 2018-2021 | 2022-2026 |
|:-------|:-----------------|:----------|:----------|:----------|
| ADHD   | Overall          |           |           |           |
| ADHD   | Clinical         |           |           |           |
| ADHD   | Lived experience |           |           |           |
| Autism | Overall          |           |           |           |
| Autism | Clinical         |           |           |           |
| Autism | Lived experience |           |           |           |

Post-hoc sentiment collocate contributors by target, frame, and period.

</div>

Note. Cells show the three largest preprocessed collocate contributors
from each direction.

<div id="tab:lsc-posthoc-arousal-contributors">

| Target | Frame            | 2014-2017 | 2018-2021 | 2022-2026 |
|:-------|:-----------------|:----------|:----------|:----------|
| ADHD   | Overall          |           |           |           |
| ADHD   | Clinical         |           |           |           |
| ADHD   | Lived experience |           |           |           |
| Autism | Overall          |           |           |           |
| Autism | Clinical         |           |           |           |
| Autism | Lived experience |           |           |           |

Post-hoc arousal collocate contributors by target, frame, and period.

</div>

Note. Cells show the three largest preprocessed collocate contributors
from each direction.

<div id="tab:lsc-posthoc-breadth-contributors">

| Target | Frame | 2014-2017 | 2018-2021 | 2022-2026 |
|:---|:---|:---|:---|:---|
| ADHD | Overall | disorder, behavior, childhood, impulsivity, inattention | disorder, include, child, symptom, syndrome | disorder, child, autism, learning, learn |
| ADHD | Clinical | disorder, behavior, childhood, impulsivity, inattention | disorder, include, syndrome, autism, disability | disorder, condition, autism, use, learning |
| ADHD | Lived experience | disorder, thing, child, parent, school | disorder, autism, student, diagnose, people | disorder, autism, dyslexia, spectrum, autistic |
| Autism | Overall | lead, need, time, society, channel | world, day, feel, high, thing | lead, research, fact, know, actually |
| Autism | Clinical | family, individual, research, speaks, government | child, early, new, organization, speaks | speaks, need, developmental, treatment, research |
| Autism | Lived experience | far, car, actually, attitude, away | world, day, feel, high, work | experience, think, people, program, speaks |

Post-hoc breadth high-distance content words by target, frame, and
period.

</div>

Note. Cells show the five most frequent filtered content lemmas among
the twenty highest-distance contexts in each cell. This breadth
diagnostic summarises high-distance context wording rather than VAD
collocates.

## ADHD Neighbour Similarity Evolution

Figure <a href="#fig:lsc-thematic-adhd-appendix" data-reference-type="ref"
data-reference="fig:lsc-thematic-adhd-appendix">8.1</a> presents the
annual similarity trajectories for the stable ADHD neighbours reported
in the Results chapter.

<figure id="fig:lsc-thematic-adhd-appendix"
data-latex-placement="!htbp">
<embed
src="lsc/thematic_evolution/lsc_thematic_neighbour_similarity_adhd.pdf" />
<figcaption>Annual cosine similarity between the ADHD concept token and
stable Word2Vec neighbours in overall, clinical/disorder, and
lived-experience contexts. Stable neighbours appeared in an annual
top-five list in at least two years and had estimates in at least ten
years.</figcaption>
</figure>

[^1]: For a detailed record of the design, configuration, and
    methodological decisions behind the Common Crawl collection pipeline
    refer to
    <https://github.com/jako6f/msc-nlp-therapy-speak/blob/main/reports/commoncrawl_corpus_design_and_provenance.md>.

[^2]: Both `warcio` and `pywb` are open-source Webrecorder projects; see
    <https://github.com/webrecorder/warcio> and
    <https://github.com/webrecorder/pywb>.

[^3]: The full code implementation of all analyses is available at
    <https://github.com/jako6f/msc-nlp-therapy-speak/tree/main/notebooks>.

[^4]: The full frame codebook is available at
    <https://github.com/jako6f/msc-nlp-therapy-speak/tree/main/notebooks/01_classification/codebooks>.
