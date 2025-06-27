---
title: Glossary
---

### Administration form

The form in which a medicine is actually given to or taken by the patient. This may differ from the [dispensing form](/faq/#dispensing-form) - for example, a soluble tablet (dispensing form) becomes a solution (administration form) when dissolved in water before being taken by the patient.

### dm+d (dictionary of medicines and devices)

The NHS standard dictionary for medicines and medical devices. It consists of 5 main classes of information and can be used to identify additional product information for products within the SCMD like grouping, form, chemical content, and administration route. Read more about it [here](https://www.bennett.ox.ac.uk/blog/2025/03/getting-more-from-the-secondary-care-medicines-data-using-the-dictionary-of-medicines-and-devices/).

### Dispensing form

The physical form in which a medicine is supplied or dispensed. This may differ from the [administration form](/faq/#administration-form) - for example, effervescent tablets are dispensed as tablets but administered as a solution after being dissolved in water.

### Dose

A specified amount of a medication to be taken at one time. E.g. 2 tablets.

### Dose Form

A [SNOMED CT code](/faq/#snomed-ct) that represents the dispensing form of a medicine. This indicates how the medicine is packaged or dispensed, rather than how it's administered to the patient.

### Dose regimen

A schedule of doses of a medicine which includes the dose, frequency and duration. E.g. 2 x 100mg tablets, twice daily, for 14 days.

### Drug route

A [SNOMED CT code](/faq/#snomed-ct) that indicates a possible way a medicine can be administered to a patient. A [VMP](/faq/#vmp-virtual-medicinal-product) can have multiple possible routes of administration, such as both intramuscular and intravenous routes.

### Ontology Form & Route

A [dm+d](/faq/#dmd-dictionary-of-medicines-and-devices)-specific code that combines both the form and route information for a medicine. For example, _solution.oral_ would be used for a soluble tablet that is taken orally.

### SCMD quantity

A lightly processed version of the raw quantity reported in the [SCMD](/faq/#secondary-care-medicines-data-scmd). Read more about how it is generated [here](https://www.bennett.ox.ac.uk/blog/2025/05/measuring-quantity-in-the-secondary-care-medicines-data/).

### Secondary Care Medicines Data (SCMD)

A dataset containing information on the quantity of medicines and devices issued by individual NHS Trusts in England. Read more about it [here](https://www.bennett.ox.ac.uk/blog/2025/02/understanding-the-secondary-care-medicines-dataset/).

### SNOMED CT

SNOMED CT (Systematized Nomenclature of Medicine - Clinical Terms) is a structured vocabulary for recording information in patient records, mandated for use by all NHS healthcare providers in England for capturing clinical information.

### Strength

Represents the amount of active ingredient in a medicinal product. Can be expressed as a single value (numerator only) or as a ratio (numerator/denominator) for solutions or concentrations. Each strength value has associated units of measure. For example, 500mg for a tablet, or 50mg/1ml for a solution.

### Unit dose

Refers to a single measured quantity of medicine that is packaged and ready for patient administration. E.g, a tablet. Read more about it [here](https://www.bennett.ox.ac.uk/blog/2025/06/calculating-unit-dose-quantity-in-the-secondary-care-medicines-data/).

### Unit Dose Form

One of 3 possible forms for the unit dose: _Discrete_ (for countable units like tablets), _Continuous_ (for measurable quantities like liquids), or _Not applicable_ (for devices like catheters where the concept of a dose isn't relevant).

### Unit dose form size (UDFS)

A numeric value indicating the amount of a medicine in a [unit dose](/faq/#unit-dose).

### Unit dose form unit of measure

The unit of measure for the [unit dose form size](/faq/#unit-dose-form-size-udfs).

### Unit dose unit of measure

The unit of measure for the unit dose. Examples include, vial, tablet and pre-filled syringe.

### VMP (Virtual Medicinal Product)

A class within the [dm+d](/faq/#dmd-dictionary-of-medicines-and-devices) representing specific medicinal products with defined forms and strengths. VMPs contain various attributes including [dose form](/faq/#dose-form) and [unit dose measurements](/faq/#unit-dose), and link to other dm+d classes for additional information. The [SCMD](/faq/#secondary-care-medicines-data-scmd) is reported at this level of the dm+d.

### VPI (Virtual Product Ingredient)

A class within the [dm+d](/faq/#dmd-dictionary-of-medicines-and-devices) containing detailed information about the ingredients present in a [VMP](/faq/#vmp-virtual-medicinal-product). It includes [strength information](/faq/#strength) through a numerator (single value) and potentially a denominator (for solutions/concentrations), along with their respective units of measure.

### VTM (Virtual Therapeutic Moiety)

The highest level of the [dm+d](/faq/#dmd-dictionary-of-medicines-and-devices) hierarchy, representing an abstract concept of medical substances or devices without specifying strength or form. A single VTM can be associated with many [VMPs](/faq/#vmp-virtual-medicinal-product) and can represent single substances or combinations of substances.