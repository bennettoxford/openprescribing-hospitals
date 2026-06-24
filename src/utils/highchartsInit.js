import Highcharts from 'highcharts';
import HighchartsMore from 'highcharts/highcharts-more';
import Exporting from 'highcharts/modules/exporting';
import ExportData from 'highcharts/modules/export-data';
import OfflineExporting from 'highcharts/modules/offline-exporting';
import Accessibility from 'highcharts/modules/accessibility';
import Boost from 'highcharts/modules/boost';
import Annotations from 'highcharts/modules/annotations';

const initializeModule = (moduleFactory) => {
  const initializer = typeof moduleFactory === 'function'
    ? moduleFactory
    : moduleFactory?.default;

  if (typeof initializer === 'function') {
    initializer(Highcharts);
  }
};

[
  HighchartsMore,
  Exporting,
  ExportData,
  OfflineExporting,
  Accessibility,
  Boost,
  Annotations
].forEach(initializeModule);

export default Highcharts;
