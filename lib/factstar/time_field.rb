#
# Class hierarchy for extracting logging values from ruby Time instances.
# Also helpful for constructing reporting queries based on these values.
#
class Factstar::TimeField

  YEAR_OF_EPOCH = 1970
  FIRST_JD_OF_EPOCH = Time.at(0).to_datetime.jd

  SECS_IN_DAY = 60 * 60 * 24
  SECS_IN_HALF_HOUR = 30 * 60
  HALF_HOURS_IN_DAY = 48
  DAYS_IN_WEEK = 7
  MONTHS_IN_YEAR = 12

  attr_reader :value

  def initialize(value)
    @value = value.to_i
  end

  def +(inc_by)
    self.class.new(@value + inc_by)
  end

  def -(dec_by)
    self + (-dec_by)
  end

  # Create an array of objects between this one and end_field (exclusive)
  def to(end_field)
    (self.value...end_field.value).map do |counter|
      self.class.new(counter)
    end
  end

  def succ
    self + 1
  end

  def ==(rhs)
    self.class == rhs.class && self.value == rhs.value
  end

  def <=>(rhs)
    @value <=> rhs.value
  end

  # Extend this to return the earliest possible time represented by the time field
  def to_time
    raise NotImplementedError
  end

  def to_s
    to_time.iso8601
  end

  def to_i
    @value
  end

  #
  # Represents the number of half hours since 1970 (the epoch)
  #
  class HalfHour < Factstar::TimeField

    def self.from_time(time)
      new(time.to_i / SECS_IN_HALF_HOUR)
    end

    def to_day
      Day.new((@value / HALF_HOURS_IN_DAY).floor)
    end

    def to_time
      Time.at(@value * SECS_IN_HALF_HOUR)
    end

    def half_hour_of_day
      ((time = to_time).hour * 2) + (time.min < 30 ? 0 : 1)
    end

    def to_half_hour_of_day
      HalfHourOfDay.new(half_hour_of_day)
    end
  end

  #
  # Represents a number of days since 1970 (the epoch)
  #
  class Day < Factstar::TimeField

    def self.from_time(time)
      new(time.to_datetime.jd - FIRST_JD_OF_EPOCH)
    end

    def to_time
      @time ||= Time.at(@value * SECS_IN_DAY)
    end

    def to_week
      Week.new(week)
    end

    def week
      (@value / DAYS_IN_WEEK).floor
    end

    # lets keep two methods (to_* and *) to keep logging light (it wants raw values)
    def day_of_week
      to_time.wday
    end

    def to_day_of_week
      DayOfWeek.new(day_of_week)
    end

    def week_of_year
      # slightly kludgey, but means we don't have to calculate the week of the year
      # ourselves (which is a PITA)
      (to_time.strftime("%j").to_i / 7).floor
    end

    def month_of_year
      to_time.month
    end

    def to_month_of_year
      MonthOfYear.new(month_of_year)
    end

    def year
      to_time.year - YEAR_OF_EPOCH
    end

    def to_year
      Year.new(year)
    end
  end

  class HalfHourOfDay < Factstar::TimeField

    def to_s
      hour_s = (@value / 2).floor.to_s.rjust(2, "0")
      min_s  = @value % 2 == 0 ? "00" : "30"

      "#{hour_s}:#{min_s}"
    end

    def self.all; new(0)..new(HALF_HOURS_IN_DAY-1); end
  end

  class DayOfWeek < Factstar::TimeField

    def to_s; Time::RFC2822_DAY_NAME[@value]; end

    def self.all; new(0)..new(DAYS_IN_WEEK-1); end
  end

  class MonthOfYear < Factstar::TimeField

    def to_s; Time::RFC2822_MONTH_NAME[@value - 1]; end

    def self.all; new(1)..new(MONTHS_IN_YEAR); end
  end

  class Year < Factstar::TimeField

    def to_s; (YEAR_OF_EPOCH + @value).to_s; end

  end

  class Week < Factstar::TimeField

    def to_time
      Time.at((DAYS_IN_WEEK * @value) * SECS_IN_DAY)
    end

  end


end

