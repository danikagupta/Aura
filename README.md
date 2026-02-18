This is the repository for the Aura Research Repurposing Framework.
# Aura (Automated Unlocking of Research Archives)

Aura is a multi-domain LLM framework designed for **Automated Research Repurposing**. It leverages state-of-the-art Natural Language and Vision Processing to transform unstructured scientific publications (PDFs) into machine-learning-ready datasets while maintaining a **Human-In-The-Loop** architecture for expert intervention.

## Framework Architecture

Aura employs a three-stage modular pipeline:

1. 
**Crawler**: Performs breadth-first searches to identify and score domain-relevant research papers based on user-supplied metrics.


2. 
**Extractor**: Uses multimodal LLMs to pull structured experimental data from text, tables, and complex figures.


3. 
**Processor**: Normalizes units, standardizes lexical naming conventions, and aligns data across varied reporting styles to produce clean structured datasets.



---

## Case Study: Pharmacogenetics (PGx)

This repository also contains the results of the Pharmacogenetics cases study, including generated dataset and modeling code.

## Case Study: Mycoremediation
This case study is available in a separate reoo (https://github.com/danikagupta/Deep-Myco)



## License

This project is open-sourced under the **MIT License** to democratize access to research archives and speed up the "flywheel of science."

