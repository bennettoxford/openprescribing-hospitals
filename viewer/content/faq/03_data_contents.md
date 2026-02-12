---
title: Data contents
---

### What is stock control data?

Stock control data is information collected from hospital pharmacy systems that is used to track and manage medicines use within hospitals. These systems support management of inventory levels, monitoring of medication usage and tracking of costs. 

Unlike primary care prescribing data, hospital stock control data is not based on individual patient prescriptions. The data represents medication distribution within hospitals, such as issuing of medicines to wards or clinical areas, rather than actual patient-level prescribing. 

### Which medicines and devices are included?

All medicines and devices used by NHS Trusts in England with a dm+d code are included. You can view detailed information about all included products using the [Product Lookup page](https://hospitals.openprescribing.net/product-lookup/).

### What is the dm+d?

The dm+d is the standard dictionary for medicines and devices used across the NHS and contains standardised codes, descriptions and metadata for individual items. You can read more about how we use it on OpenPrescribing Hospitals in our blog post, [Getting more from the secondary care medicines data using the dictionary of medicines and devices](https://www.bennett.ox.ac.uk/blog/2025/03/getting-more-from-the-secondary-care-medicines-data-using-the-dictionary-of-medicines-and-devices/).

### Is homecare medicines data included?

Some medicines that are issued by hospitals can be delivered directly to patients' homes. The official release guidance for the SCMD does not indicate whether this data is included in the dataset. However, in the methodology of the [NICE Technology Appraisal Innovation Scorecard](https://www.nhsbsa.nhs.uk/statistical-collections/nice-technology-appraisals/nice-technology-appraisals-nhs-england-innovation-scorecard-december-2023/estimates-report), which uses the SCMD and is published by the NHS Business Services Authority says the following:

> Although the secondary care database captures some data for drugs supplied through homecare services, it is incomplete. Therefore, the actual volume of medicine used may be higher than the volume reported in the observed use.

If you know more details about homecare medicines in the SCMD, please [let us know](https://hospitals.openprescribing.net/contact/).


### Why can't I find the medicine I am looking for?

There are several possible reasons:

* You're searching for a brand name which are not available in the SCMD. Search for generic names of the medicine you're looking for.
* It might not be issued in hospitals. Some medicines are only used in primary care.
* It might not have been prescribed within the NHS before.
* What you're looking for is a non-standardised item - where specific medicines do not have a dm+d code, they cannot be standardised across organisations and are not included within the SCMD. Examples include clean room consumables and packaging items.
* The medicine is dispensed by community pharmacy. NHS prescriptions supplied by hospitals but dispensed in community pharmacy are not included in the SCMD, but this data is available separately. This may be incorporated into the OpenPrescribing Hospitals platform in the future.

### Is data for biosimilars available?

Products are reported in the SCMD at the level of [Virtual Medicinal Products](/faq/#what-is-a-vmp). This does not give any indication of whether a product is issued as a biosimilar or not. This would require products to be reported at the level of Actual Medicinal Products (AMPs), which are atributed to specific manufacturers. You can read more in our blog post, [You asked: Can we monitor the usage of biosimilars in secondary care using OpenPrescribing Hospitals?](https://www.bennett.ox.ac.uk/blog/2025/07/you-asked-can-we-monitor-the-usage-of-biosimilars-in-secondary-care-using-openprescribing-hospitals/).

### Which NHS Trusts are included?

The platform includes data from all NHS hospital trusts in England that submit data to Rx-Info. This includes all NHS Acute, Teaching, Specialist, Mental Health and Community Trusts in England (this does not include medications dispensed by community pharmacy but this is [available separately](https://opendata.nhsbsa.net/dataset/hospital-prescribing-dispensed-in-the-community)).

### How does the organisation search work?

When you search for NHS Trusts (or Integrated Care Boards or regions), the results match your search in several ways:

- **Trust name** — You can type part of the trust name. Each word you type is matched separately, so "guys thomas" will find "Guy's and St Thomas' NHS Foundation Trust". You don't have to type the complete word (e.g. "king" matches "Kings"). You do not need to type the full name or "NHS Foundation Trust"; the search ignores those suffixes.
- **Initials** — You can search by the first letters of the main words in the name. For example, "Guy's and St Thomas'" can be returned by typing "gst"; common words like "and" are ignored when building initials.
- **Organisation code** — If an organisation code is available, it is included in the search, so you can find a trust by its code.
- **Predecessor names** — Trusts that have merged or been renamed are also matched by their previous names and codes, so you can find the current trust using an old name.

Results are ordered by how well they match (more matching words first), then alphabetically by name. 

### Why can't I find my hospital on the platform?

There are multiple reasons your hospital may not be present in the data presented. These include:

* **Data is reported at the level of individual NHS Trusts**. NHS Trusts can be made up of multiple sites. If you can't find your hospital, try searching for it by its NHS Trust name.
* **NHS Trusts sometimes undergo organisational change, such as mergers or acquisitions**. The hospital you are looking for could be included in the dataset under a different name to what you might expect. Data for NHS Trusts which are now part of another NHS Trust is aggregated into the successor organisation. You can find all NHS Trusts historically included in the SCMD, with an indication of their current NHS Trust on the [Submission History page](https://hospitals.openprescribing.net/submission-history/).


### Why are there negative values for some products?

The SCMD contains pharmacy stock control data representing medication distribution within hospitals, such as issuing of medicines to wards or clinical areas, rather than actual patient-level prescribing. Not all of the stock that is issued within hospitals eventually ends up being used. In some hospitals, when this is the case and where their stock control system supports it, historical stock issues can be updated. This is known as _Backtracking_. Where supply made in a previous month is returned in a subsequent month, and the quantity returned is greater than the quantity issued in that month, the quantity reported will be negative.


You can read about this in our blog, [More about hospital stock control data](https://www.bennett.ox.ac.uk/blog/2025/02/more-about-hospital-stock-control-data/).

### What is a VMP?

VMP is short for Virtual Medicinal Product. This is a component of the [dictionary of medicines and devices (dm+d)](https://www.bennett.ox.ac.uk/blog/2019/08/what-is-the-dm-d-the-nhs-dictionary-of-medicines-and-devices/), the standard dictionary for medicines and devices used across the NHS and contains standardised codes, descriptions and metadata for individual items. VMPs describe a general class of medicines or device that may be available as an actual product. All of the data within the SCMD is reported at the level of VMPs. We commonly refer to these as _products_. 

<img src="/static/faq/vmp.png" 
     alt="dm+d hierarchy" 
     style="max-height: 26rem; margin-left: auto; margin-right: auto; max-width: 100%;" />

You can read more about VMPs in our blogs, [Understanding the secondary care medicines dataset](https://www.bennett.ox.ac.uk/blog/2025/02/understanding-the-secondary-care-medicines-dataset/) and [Getting more from the secondary care medicines data using the dictionary of medicines and devices](https://www.bennett.ox.ac.uk/blog/2025/03/getting-more-from-the-secondary-care-medicines-data-using-the-dictionary-of-medicines-and-devices/).

### How do we identify if a VMP is unlicensed?

A VMP is identified as unlicensed when all of its associated Actual Medicinal Products (AMPs) have an availability restriction of 'Special' in the [dm+d](/faq/#what-is-the-dmd). 

A VMP is marked as unlicensed only when:

* The VMP has at least one associated AMP
* **All** AMPs associated with the VMP have an availability restriction of 'Special'

If a VMP has multiple AMPs and only some of them are marked as 'Special', the VMP itself will not be identified as unlicensed. You can view whether a VMP is unlicensed on the [Product Lookup page](/product-lookup/).

### What is the Anatomical Therapeutic Chemical (ATC)/ Defined Daily Dose (DDD) system?

The ATC/DDD system is a way for classifying and measuring drug utilisation to enable comparisons of drug use between countries, regions, and other healthcare settings. It classifies medicinal products using the [Anatomical Therapeutic Classification (ATC) system](/faq/#what-is-the-anatomical-therapeutic-chemical-atc-system) and provides the [Defined Daily Dose (DDD)](/faq/#what-is-a-defined-daily-dose-ddd) as a way to measure their usage.

It is maintained by the [World Health Organisation (WHO) Collaborating Centre for Drug Statistics Methodology](https://atcddd.fhi.no/atc_ddd_methodology/who_collaborating_centre/) and is widely used internationally.

### What is the Anatomical Therapeutic Chemical (ATC) system?

The Anatomical Therapeutic Chemical (ATC) system provides a classification for the active ingredients within medicinal substances based on where in the body they act and their therapeutic (what condition or disease they treat), pharmacological (how they work in the body), and chemical properties (the structure and composition of the substance).

The system is a hierarchy with five levels of increasing specificity, which you can read more about in our blog post, [Classifying and measuring medicines usage with the ATC/DDD system](https://www.bennett.ox.ac.uk/blog/2025/07/calculating-defined-daily-dose-quantity-in-the-secondary-care-medicines-data/). Below is an example of how the ATC code for olanzapine is structured:

<img src="/static/faq/atc.png" 
     alt="ATC hierarchy showing the 5-level classification system from Anatomical main group (N) down to Chemical substance (03)" 
     style="height: clamp(5rem, 14vw, 12rem); margin-left: auto; margin-right: auto; max-width: 100%;" />


### What is a Defined Daily Dose (DDD)?

The [ATC system](/faq/#what-is-the-anatomical-therapeutic-chemical-atc-system) provides a way to classify drugs, but not a unit to measure them in. This is what the Defined Daily Dose (DDD) is for. DDDs are a unit of measure for medicines consumption that enables comparison of usage across groups of medicines. They are defined and maintained by the World Health Organization (WHO), who define them as:

> Defined Daily Dose (DDD): The assumed average maintenance dose per day for a drug used for its main indication in adults

*Source: [World Health Organization](https://www.who.int/tools/atc-ddd-toolkit/about-ddd)*

The DDD quantity for a product can be calculated as follows:

<img src="/static/faq/ddd-calc.png" 
     alt="DDD calculation logic" 
     style="max-height: 12rem; margin-left: auto; margin-right: auto; max-width: 100%;" />


### What is indicative cost?

The SCMD includes the indicative cost for each medicine or device within the dataset. The indicative cost is based on the community pharmacy reimbursement prices for generic and list prices for branded medicines. This does not reflect the actual cost paid by hospitals. This, as described in the SCMD release guidance, overestimates the total spend by a factor of two (though this overestimation is not uniform across all products):

> The indicative cost in this data set will overestimate the total spend on medicines issued in hospitals. For example, the total indicative cost of medicines issued in secondary care for 2020 / 21 was £14.5 billion compared to the net actual cost of £7.59 billion (excluding central rebates).

### Does the data include prescriptions dispensed in community pharmacy?

The SCMD does not include NHS prescriptions supplied by the hospital and dispensed in community pharmacy (hospital FP10 prescriptions). This data is available in a separate dataset, [Hospital Prescribing Dispensed in the Community](https://opendata.nhsbsa.net/dataset/hospital-prescribing-dispensed-in-the-community), which we do not currently use on OpenPrescribing Hospitals. Medicines dispensed in community pharmacy make up a very small proportion of hospital medicines usage; in January 2025, medicines dispensed in community pharmacy accounted for £5.6m actual cost, which is a very small proportion of the near £2.1bn indicative cost in SCMD data for the same month (likely ~£1bn actual cost).
