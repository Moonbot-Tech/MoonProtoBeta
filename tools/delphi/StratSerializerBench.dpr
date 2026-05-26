program StratSerializerBench;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.Diagnostics,
  StrategySerializer in 'X:\proj-X\MoonBot\src\MoonProto\StrategySerializer.pas',
  Strategies in 'X:\proj-X\MoonBot\src\Strategies.pas',
  MarketsU;

function GetPayloadPath: string;
begin
  If ParamCount >= 1 then begin
    Result := ParamStr(1);
    exit;
  end;
  Result := GetEnvironmentVariable('MOONPROTO_STRAT_SNAPSHOT_BENCH');
  If Result.IsEmpty then
    raise Exception.Create('usage: StratSerializerBench <TStratSnapshot.Data.bin> [iters]');
end;

function GetIterations: Integer;
begin
  Result := 500;
  If ParamCount >= 2 then
    Result := StrToInt(ParamStr(2));
  If Result <= 0 then
    raise Exception.Create('iters must be positive');
end;

procedure FreeStrategyItems(BenchStrats: TStrategies);
begin
  for var sg in BenchStrats do
    sg.Free;
  BenchStrats.Clear;
end;

procedure LoadPayload(const Path: string; Payload: TMemoryStream);
begin
  Payload.Clear;
  Payload.LoadFromFile(Path);
  Payload.Position := 0;
end;

procedure LoadOnce(BenchStrats: TStrategies; Payload: TMemoryStream);
var
  Ser: TStrategySerializer;
begin
  Payload.Position := 0;
  Ser := TStrategySerializer.Create;
  try
    // TStrategy.Create -> rebuildProps reads the global Strategies.Strats list.
    // Keep it pointed at the same container as the live MoonBot code.
    Strategies.Strats := BenchStrats;
    Ser.LoadStrategiesFromStream(BenchStrats, Payload);
  finally
    Ser.Free;
  end;
end;

procedure MeasureCold(const Path: string; Iterations: Integer);
var
  Payload: TMemoryStream;
  BenchStrats: TStrategies;
  Watch: TStopwatch;
  TotalTicks: Int64;
  MaxTicks: Int64;
  ElapsedTicks: Int64;
  Checksum: Int64;
begin
  Payload := TMemoryStream.Create;
  try
    LoadPayload(Path, Payload);
    TotalTicks := 0;
    MaxTicks := 0;
    Checksum := 0;
    for var i := 1 to Iterations do begin
      BenchStrats := TStrategies.Create;
      Strategies.Strats := BenchStrats;
      Watch := TStopwatch.StartNew;
      try
        LoadOnce(BenchStrats, Payload);
        Watch.Stop;
        ElapsedTicks := Watch.ElapsedTicks;
        Inc(TotalTicks, ElapsedTicks);
        If ElapsedTicks > MaxTicks then
          MaxTicks := ElapsedTicks;
        Inc(Checksum, BenchStrats.Count);
      finally
        Strategies.Strats := nil;
        FreeStrategyItems(BenchStrats);
        BenchStrats.Free;
      end;
    end;
    Writeln(Format(
      'DELPHI_STRAT_BENCH cold iters=%d avg/max=%dus/%dus checksum=%d',
      [
        Iterations,
        Round((TotalTicks / Iterations) * 1000000.0 / TStopwatch.Frequency),
        Round(MaxTicks * 1000000.0 / TStopwatch.Frequency),
        Checksum
      ]));
  finally
    Payload.Free;
  end;
end;

procedure MeasureWarm(const Path: string; Iterations: Integer);
var
  Payload: TMemoryStream;
  BenchStrats: TStrategies;
  Watch: TStopwatch;
  TotalTicks: Int64;
  MaxTicks: Int64;
  ElapsedTicks: Int64;
  Checksum: Int64;
begin
  Payload := TMemoryStream.Create;
  BenchStrats := TStrategies.Create;
  Strategies.Strats := BenchStrats;
  try
    LoadPayload(Path, Payload);
    LoadOnce(BenchStrats, Payload);
    TotalTicks := 0;
    MaxTicks := 0;
    Checksum := 0;
    for var i := 1 to Iterations do begin
      Watch := TStopwatch.StartNew;
      LoadOnce(BenchStrats, Payload);
      Watch.Stop;
      ElapsedTicks := Watch.ElapsedTicks;
      Inc(TotalTicks, ElapsedTicks);
      If ElapsedTicks > MaxTicks then
        MaxTicks := ElapsedTicks;
      Inc(Checksum, BenchStrats.Count);
    end;
    Writeln(Format(
      'DELPHI_STRAT_BENCH warm iters=%d avg/max=%dus/%dus checksum=%d',
      [
        Iterations,
        Round((TotalTicks / Iterations) * 1000000.0 / TStopwatch.Frequency),
        Round(MaxTicks * 1000000.0 / TStopwatch.Frequency),
        Checksum
      ]));
  finally
    Strategies.Strats := nil;
    FreeStrategyItems(BenchStrats);
    BenchStrats.Free;
    Payload.Free;
  end;
end;

var
  Path: string;
  Iterations: Integer;
  Payload: TMemoryStream;
begin
  try
    If Markets = nil then
      Markets := TMarkets.Create;
    Path := GetPayloadPath;
    Iterations := GetIterations;
    Payload := TMemoryStream.Create;
    try
      LoadPayload(Path, Payload);
      Writeln(Format(
        'DELPHI_STRAT_BENCH payload=%s bytes=%d iters=%d',
        [Path, Payload.Size, Iterations]));
    finally
      Payload.Free;
    end;
    MeasureCold(Path, Iterations);
    MeasureWarm(Path, Iterations);
  except
    on E: Exception do begin
      Writeln('ERROR: ' + E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
