---
title: Measures
---

### How are measures calculated?

Measures can simply be a count of the amount of a drug being issued but more commonly a ratio is used. To calculate a ratio, we specify a list of products to include in the denominator and the subset of these products which make up the numerator. We then sum the quantity issued across the numerator and denominator separately, divide the numerator by the denominator and multiply by 100 to get a percentage.

You can read more about measures in our blog, [Introducing OpenPrescribing Hospitals measures](https://www.bennett.ox.ac.uk/blog/2025/04/introducing-openprescribing-hospitals-measures/).

### Is lower better?

We are often able to make a value judgement — based on guidance, evidence or national priorities underpinning the measure — to determine what represents "better" practice. When we can do this, we aim to present measures using a "lower is better" principle to ensure consistency with how data is displayed. To achieve this we may need to design and phrase measures [slightly differently to what you may have seen elsewhere](https://www.bennett.ox.ac.uk/blog/2024/01/spot-the-difference-part-1-source-datasets/). For example, for the [Best value DOACs measure](/measures/best_value_doacs/), instead of showing the percentage of DOACs prescribed as the preferred choice, we display the percentage of DOACs that are NOT prescribed as the preferred choices.

For some measures it is not always possible to determine what is "better," as this can depend on various local factors, so we cannot apply a value judgement. For such measures, we present the data as it is, allowing users to interpret it in the context of their specific circumstances.

### How is "potential for improvement" and "most improved" determined?

**Potential for improvement** ranks trusts based on their percentile position over the last 12 months. A trust at a high percentile (e.g. 90th) for a "lower is better" measure has more potential to improve than one at the 50th percentile.

**Most improved** compares the average of the trust's percentile rank in the first 3 months of the 12‑month period to the average in the most recent 3 months. A drop in average percentile (for "lower is better" measures) or rise (for "higher is better" measures) counts as improvement.

These metrics are not calculated for measures where we cannot make a value judgement (no "lower is better" or "higher is better"). These measures will appear at the end, when sorting by one of these metrics.

### What are percentile charts?

Percentile charts show the extent of variation in medication use at the level of individual trusts. You can read more about why we use them in our blogs, [communicating variation in prescribing](https://www.bennett.ox.ac.uk/blog/2019/04/communicating-variation-in-prescribing-why-we-use-deciles/) and [highlighting variation in hospitals medicines usage](https://www.bennett.ox.ac.uk/blog/2025/04/highlighting-variation-in-hospitals-medicines-usage).

### How should I interpret percentiles for measures without denominators?

Measures with a denominator are normalised and comparable across trusts of different sizes. However, for measures without denominators and in custom analyses, percentiles show variation in absolute quantities. In these cases, variation in percentiles can simply reflect differences in hospital size rather than genuine variation in what is being measured. When interpreting percentiles for measures without denominators or custom analyses, consider whether differences might be explained by the size or activity level of the trusts rather than differences in issuing patterns.

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

### Can I see patterns in a single measure for each trust individually?

From the measures list, when you have **NHS Trust** selected in `View Mode`, each measure card shows a **View measure for all trusts** link. This opens a page that shows the pattern for each trust against percentiles calculated across all other trusts included in that measure.