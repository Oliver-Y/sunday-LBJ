# Legal Decision Prediction Data Sources

To bootstrap a system that predicts judicial decisions and reasoning, you need a broad set of inputs that cover docket metadata, filings, opinions, and contextual information about judges and courts. Below is a curated list of publicly accessible sources that are commonly used in the U.S. legal domain.

## Court Filings and Opinions
- **CourtListener (Free Law Project)**: Offers bulk opinions, oral argument audio, PACER-derived RECAP dockets, and judicial metadata via APIs and bulk downloads. Useful for retrieving full-text opinions, docket events, and judge assignments.
- **RECAP Archive (via CourtListener)**: Crowdsourced PACER documents including complaints, motions, orders, and briefs. Provides chronological docket information and filings.
- **GovInfo**: Official repository for federal court opinions (especially appellate) and other government documents, including Congressional and regulatory materials.
- **U.S. Supreme Court (SCOTUS) Data**: Official website offers slip opinions and case information. The Supreme Court Database (SCDB) supplies structured case-level variables for modeling outcomes.

## Judge and Court Metadata
- **Federal Judicial Center (FJC) Biographical Directory**: Contains judge biographies, appointment dates, and prior experience to enrich judge-level features.
- **Ballotpedia & Wikipedia**: Supplemental biographical details and career timelines for judges at state and federal levels.
- **Administrative Office of the U.S. Courts**: Publishes caseload statistics and court structure data helpful for contextual features.

## Oral Arguments and Transcripts
- **Oyez**: Audio and transcripts for U.S. Supreme Court arguments, plus summaries and vote breakdowns.
- **CourtListener Oral Arguments**: Provides audio recordings and metadata for many federal appellate courts.

## Secondary Sources and Enrichment
- **Harvard Caselaw Access Project (CAP)**: Historical corpus of U.S. state and federal case law with metadata; offers API and bulk access for research purposes.
- **LawArXiv / SSRN**: Research papers and legal analysis for augmenting textual corpora or building embeddings of legal reasoning.
- **LegisPro / Congress.gov**: Legislative history, statutes, and bill summaries for legal context.

## Data Integration Tips
- Start with structured metadata (SCDB, FJC, CourtListener judges API) to define entities and features.
- Layer in docket events and filings from RECAP to model procedural posture and parties.
- Use opinion texts and oral arguments for natural-language models that capture reasoning.
- Track identifiers (docket numbers, PACER case IDs, SCDB IDs) to join datasets across sources.

Each source has its own licensing and access limitations. Review terms of service—especially for PACER/RECAP documents—and plan for data storage, normalization, and anonymization where necessary.
