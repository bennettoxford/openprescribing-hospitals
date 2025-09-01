---
title: Analyse
---

### What does quantity mean?

Quantity is the total amount of a medicine that has been issued as reported in the SCMD. The unit of measure used to measure the quantity varies across different products. Where possible, units are mapped to a consistent unit basis (e.g. quantities reported in micrograms and milligrams are both converted to quantity in grams) to allow for comparison between different products. Quantities reported in different unit bases may not be comparable. For this reason, we calculate alternative measures of quantity from the quantity reported in the SCMD. These include [SCMD quantity](/faq/#what-is-scmd-quantity), [unit dose quantity](/faq/#what-is-unit-dose-quantity), [ingredient quantity](/faq/#what-is-ingredient-quantity) and [DDD quantity](/faq/#what-is-ddd-quantity).

### Why is there no quantity for some products?

There is no quantity reported when a trust has not issued a product or the selected [quantity type](/faq/#what-does-quantity-mean) is not available for the product. See [How is the quantity type used for an analysis chosen?](faq/#how-is-the-quantity-type-used-for-an-analysis-chosen).

### How do I see ICB, regional, and national breakdowns?

To view ICB, regional, and national analysis breakdowns, as well as national trust-level variation as a percentiles chart, run an analysis **without selecting any NHS Trusts** in the analysis builder. These geographic breakdown modes are only available when analysing data across all trusts.

### How do I restrict an analysis to a specific set of NHS Trusts?

To restrict an analysis to a specific set of NHS Trusts, select the NHS Trusts you want to include in the analysis builder. This will filter the analysis to show only data from your selected trusts.

### What is SCMD quantity?

SCMD quantity is a normalised version of the raw [quantity](/faq/#what-does-quantity-mean) available in the SCMD. This quantity measure is available for all products reported in the SCMD. You can read more about how it is calculated in our blog post, [Measuring quantity in the Secondary Care Medicines Data](https://www.bennett.ox.ac.uk/blog/2025/05/measuring-quantity-in-the-secondary-care-medicines-data/). For examples of reported SCMD quantities, search for a product on the [Product Lookup page](https://hospitals.openprescribing.net/product-lookup/).

### What is unit dose quantity?

A unit dose is a single measured quantity of a medicine, packaged for administration to a patient at one time. Unit dose quantity is the equivalent of the SCMD quantity in terms of the number of unit doses issued. This measure of quantity is not available for all products. You can read more about how it is calculated and the products it is available for in our blog post, [Calculating unit dose quantity in the Secondary Care Medicines Data](https://www.bennett.ox.ac.uk/blog/2025/06/calculating-unit-dose-quantity-in-the-secondary-care-medicines-data/). Some example unit dose calculations are shown below. To see if, and how the unit dose quantity is calculated for any product included in the SCMD, search for a product on the [Product Lookup page](https://hospitals.openprescribing.net/product-lookup/).

<img src="/static/faq/unit-doses.png" 
     alt="Unit dose calculation" 
     style="max-height: 20rem; margin-left: auto; margin-right: auto; max-width: 100%;" />


### What is ingredient quantity?

Using the [dm+d](/faq/#what-is-the-dmd) the individual ingredients within a product can be identified. Ingredient quantity is the quantity of each ingredient within a product that is equivalent to the SCMD quantity. Not all products have specified ingredients, and for those that do, it is not always possible to determine the ingredient quantity. You can read more about how ingredient quantity is calculated and the products it is available for in our blog post, [Calculating ingredient quantity in the Secondary Care Medicines Data](https://www.bennett.ox.ac.uk/blog/2025/06/calculating-ingredient-quantity-in-the-secondary-care-medicines-data/). Some example ingredient calculations are shown below. To see if, and how the ingredient quantity is calculated for any product included in the SCMD, search for a product on the [Product Lookup page](https://hospitals.openprescribing.net/product-lookup/).

<img src="/static/faq/ingredient-quantity.png" 
     alt="Ingredient quantity calculation" 
     style="max-height: 20rem; margin-left: auto; margin-right: auto; max-width: 100%;" />

### What is DDD quantity?

[Defined Daily Dose (DDD)](/faq/#what-is-a-ddd) is a unit of measure for medicines consumption that enables comparison of usage across groups of medicines. DDD quantity is the equivalent of the SCMD quantity in terms of the number of DDDs issued. This measure of quantity is not available for all products. You can read more about how DDD quantity is calculated and the products it is available for in our blog post, [Calculating DDD quantity in the Secondary Care Medicines Data](https://www.bennett.ox.ac.uk/blog/2025/06/calculating-ddd-quantity-in-the-secondary-care-medicines-data/). Some example DDD calculations are shown below. To see if, and how the DDD quantity is calculated for any product included in the SCMD, search for a product on the [Product Lookup page](https://hospitals.openprescribing.net/product-lookup/).

<img src="/static/faq/ddd-calc-examples.png" 
     alt="DDD calculation" 
     style="max-height: 20rem; margin-left: auto; margin-right: auto; max-width: 100%;" />

### How is the quantity type used for an analysis chosen?

When you select products for analysis, the most appropriate quantity type to use is automatically selected based on the information available for the products. The selection is designed to improve the likelihood that the quantities being compared are clinically meaningful and that quantity data is available for the greatest number of the selected products.

<img src="/static/faq/choosing-quantity-type.png" 
     alt="Quantity type selectionlogic" 
     style="max-height: 52rem; margin-left: auto; margin-right: auto; max-width: 100%;" />

There may be some selections of products where the chosen quantity type is not what you want for your analysis. You can change the quantity type in the advanced options of the analysis builder, but pay attention to any additional warning messages indicating the appropriateness of the comparison. To see what quantity types are available for your selected products, use the [Product Lookup](/product-lookup/) tool.

