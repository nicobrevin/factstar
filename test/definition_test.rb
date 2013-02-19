describe 'defining a schema' do
  describe 'registering fact types' do
    new_schema do
      dimension_type(:user, :table => :users) do
        dimension :id,    :integer, :column_name => :user_id, :primary => true
        dimension :login, :string,  :group_by => false, :sort_by => true
      end
    end

  end

  describe 'registering dimension types' do
  end

  describe 'registering value types' do
  end
end
