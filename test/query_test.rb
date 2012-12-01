describe 'executing queries' do

  describe 'aggregate query' do
    it 'can be grouped by a single dimension'
    it 'can be grouped by multiple dimensionss'
    it 'can be filtered by a dimension'
  end

  describe 'second aggregate query' do
    it 'can be grouped by a second dimension that was a pair-wise dependency ' +
      'of one of the first groupings'
  end

  describe 'distribution query' do
    it 'can distribute the different integer or float values of a query in to' +
      ' different buckets'
  end
end
