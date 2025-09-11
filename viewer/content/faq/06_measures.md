---
title: Measures
---

### How are measures calculated?

Measures can simply be a count of the amount of a drug being issued but more commonly a ratio is used. To calculate a ratio, we specify a list of products to include in the denominator and the subset of these products which make up the numerator. We then sum the quantity issued across the numerator and denominator separately, divide the numerator by the denominator and multiply by 100 to get a percentage.

You can read more about measures in our blog, [Introducing OpenPrescribing Hospitals measures](https://www.bennett.ox.ac.uk/blog/2025/04/introducing-openprescribing-hospitals-measures/).

### What are percentile charts?

Percentile charts show the extent of variation in medication use at the level of individual trusts. You can read more about why we use them in our blogs, [communicating variation in prescribing](https://www.bennett.ox.ac.uk/blog/2019/04/communicating-variation-in-prescribing-why-we-use-deciles/) and [highlighting variation in hospitals medicines usage](https://www.bennett.ox.ac.uk/blog/2025/04/highlighting-variation-in-hospitals-medicines-usage).

### What does a value of zero mean in a measure?

When viewing measures, you may see trusts with a value of zero for certain months. This can happen for two main reasons:

**1. No usage:** The trust did not issue the relevant products during that month.

**2. No data submission:** The trust did not submit data for the relevant products to the SCMD for that month.

We cannot distinguish between true zeroes and zeroes resulting from missing data submissions. You can see the submission history for an trust on the [Submission History](https://hospitals.openprescribing.net/submission-history/) page to get an idea of the completeness of the data for a specific trust.

### Why do some trusts have out of range values?

Within the SCMD, it is possible for issued quantity to be negative (see [Why are there negative values for some products?](faq/#why-are-there-negative-values-for-some-products)). When calculting measures with both a numerator and denominator, values are expected to be between 0% and 100%. However, negative values can result in the numerator being greater than the denominator (so the measure value can be greater than 100%) or the denominator being negative (so the measure value can be less than 0%).

### What do annotations on measure charts show?

Measure annotations indicate events that may have influenced issuing patterns of products included a given measure. They help to provide context for changes in the data over time.

For example, annotations might mark:
- Patent expiries that could affect product choice
- Changes in clinical guidance
- Introduction of new treatments
- Policy changes affecting issuing

### Can I hide annotations on measure charts?

To hide annotations on a measure chart:

1. Click the chart menu in the top-right corner of the chart
2. Select **"Hide annotations"** from the dropdown menu

To show annotations again, repeat the same steps and select **"Show annotations"**.