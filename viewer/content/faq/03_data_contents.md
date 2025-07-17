---
title: Data contents
---

### What is stock control data?

Stock control data is information collected from hospital pharmacy systems that is used to track and manage medicines use within hospitals. These systems support management of inventory levels, monitoring of medication usage and tracking of costs. 

Unlike primary care prescribing data, hospital stock control data is not based on individual patient prescriptions. The data represents medication distribution within hospitals, such as issuing of medicines to wards or clinical areas, rather than actual patient-level prescribing. 

### Which medicines and devices are included?

All medicines and devices used by NHS Trusts in England with a dm+d code are included.

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

### Which NHS Trusts are included?

The platform includes data from all NHS hospital trusts in England that submit data to Rx-Info. This includes all NHS Acute, Teaching, Specialist, Mental Health and Community Trusts in England (this does not include medications dispensed by community pharmacy but this is [available separately](https://opendata.nhsbsa.net/dataset/hospital-prescribing-dispensed-in-the-community)).

### Why can't I find my hospital on the platform?

There are multiple reasons your hospital may not be present in the data presented. These include:

* **Data is reported at the level of individual NHS Trusts**. NHS Trusts can be made up of multiple sites. If you can't find your hospital, try searching for it by its NHS Trust name.
* **NHS Trusts sometimes undergo organisational change, such as mergers or acquisitions**. The hospital you are looking for could be included in the dataset under a different name to what you might expect. Data for NHS Trusts which are now part of another NHS Trust is aggregated into the successor organisation. You can find all NHS Trusts historically included in the SCMD, with an indication of their current NHS Trust on the [Submission History page](https://hospitals.openprescribing.net/submission-history/).


### Why are there negative values for some products?

The SCMD contains pharmacy stock control data representing medication distribution within hospitals, such as issuing of medicines to wards or clinical areas, rather than actual patient-level prescribing. Not all of the stock that is issued within hospitals eventually ends up being used. In some hospitals, when this is the case and where their stock control system supports it, historical stock issues can be updated. This is known as _Backtracking_. Where supply made in a previous month is returned in a subsequent month, and the quantity returned is greater than the quantity issued in that month, the quantity reported will be negative.


You can read about this in our blog, [More about hospital stock control data](https://www.bennett.ox.ac.uk/blog/2025/02/more-about-hospital-stock-control-data/).

### What is a VMP?

VMP is short for Virtual Medicinal Product. This is a component of the [dictionary of medicines and devices (dm+d)](https://www.bennett.ox.ac.uk/blog/2019/08/what-is-the-dm-d-the-nhs-dictionary-of-medicines-and-devices/), the standard dictionary for medicines and devices used across the NHS and contains standardised codes, descriptions and metadata for individual items. VMPs describe a general class of medicines or device that may be available as an actual product. All of the data within the SCMD is reported at the level of VMPs. We commonly refer to these as _products_. 

You can read more about VMPs in our blogs, [Understanding the secondary care medicines dataset](https://www.bennett.ox.ac.uk/blog/2025/02/understanding-the-secondary-care-medicines-dataset/) and [Getting more from the secondary care medicines data using the dictionary of medicines and devices](https://www.bennett.ox.ac.uk/blog/2025/03/getting-more-from-the-secondary-care-medicines-data-using-the-dictionary-of-medicines-and-devices/).

### What is a DDD?

Defined Daily Dose (DDD) is a unit of measure for medicines consumption that enables comparison of usage across groups of medicines. They are defined and maintained by the World Health Organization (WHO), who define them as:

> Defined Daily Dose (DDD): The assumed average maintenance dose per day for a drug used for its main indication in adults

*Source: [World Health Organization](https://www.who.int/tools/atc-ddd-toolkit/about-ddd)*

### What is indicative cost?

The SCMD includes the indicative cost for each medicine or device within the dataset. The indicative cost is based on the community pharmacy reimbursement prices for generic and list prices for branded medicines. This does not reflect the actual cost paid by hospitals. This, as described in the SCMD release guidance, overestimates the total spend by a factor of two (though this overestimation is not uniform across all products):

> The indicative cost in this data set will overestimate the total spend on medicines issued in hospitals. For example, the total indicative cost of medicines issued in secondary care for 2020 / 21 was £14.5 billion compared to the net actual cost of £7.59 billion (excluding central rebates).


### Does the data include prescriptions dispensed in community pharmacy?

The SCMD does not include NHS prescriptions supplied by the hospital and dispensed in community pharmacy (hospital FP10 prescriptions). This data is available in a separate dataset, [Hospital Prescribing Dispensed in the Community](https://opendata.nhsbsa.net/dataset/hospital-prescribing-dispensed-in-the-community), which we do not currently use on OpenPrescribing Hospitals. Medicines dispensed in community pharmacy make up a very small proportion of hospital medicines usage; in January 2025, medicines dispensed in community pharmacy accounted for £5.6m actual cost, which is a very small proportion of the near £2.1bn indicative cost in SCMD data for the same month (likely ~£1bn actual cost). 
