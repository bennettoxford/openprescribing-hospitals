import Highcharts from 'highcharts';
import HighchartsMore from 'highcharts/highcharts-more';
import Exporting from 'highcharts/modules/exporting';
import ExportData from 'highcharts/modules/export-data';
import OfflineExporting from 'highcharts/modules/offline-exporting';
import Accessibility from 'highcharts/modules/accessibility';
import Boost from 'highcharts/modules/boost';
import Annotations from 'highcharts/modules/annotations';

for (const module of [HighchartsMore, Exporting, ExportData, OfflineExporting, Accessibility, Boost, Annotations]) {
  const initialise = module?.default ?? module;
  if (typeof initialise === 'function') {
    initialise(Highcharts);
  }
}

Highcharts.setOptions({
  colorScheme: 'light',
  chart: {
    backgroundColor: '#ffffff',
    plotBackgroundColor: '#ffffff'
  }
});

export default Highcharts;
