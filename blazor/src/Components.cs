using System.Text.Json;
using Microsoft.JSInterop;
namespace ReactiveWeb.Blazor;

public sealed class ReactiveProvider : BrowserProvider { }
public sealed class ReactiveModule(IJSRuntime js) : BrowserModule(js)
{
    public ValueTask<ReactiveModel> CreateModelAsync(object? values = null) => ReactiveModel.CreateAsync(this, values);
    public ValueTask<BrowserCommand<TInput, TOutput>> CreateCommandAsync<TInput, TOutput>(object execute, bool asynchronous = true, bool enabled = true, bool passSignal = false) => BrowserCommand<TInput, TOutput>.CreateAsync(this, execute, asynchronous, enabled, passSignal);
}
public abstract class ReactiveResource(BrowserModule module, IJSObjectReference handle) : IAsyncDisposable
{
    public BrowserModule Module { get; } = module;
    public IJSObjectReference Handle { get; } = handle;
    private int _disposed;
    protected static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };
    protected static Task Dispatch<T>(JsonElement notification, Func<T, Task> next, Func<JsonElement, Task>? error)
    {
        if (notification.ValueKind == JsonValueKind.Object && notification.TryGetProperty("kind", out var kind) && kind.GetString() == "value") return next(notification.GetProperty("value").Deserialize<T>(Json)!);
        if (notification.ValueKind == JsonValueKind.Object && notification.TryGetProperty("error", out var failure)) return error is not null ? error(failure) : Task.FromException(new JSException(failure.ToString()));
        return Task.CompletedTask;
    }
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try { await Module.ReleaseAsync(Handle); }
        catch (ObjectDisposedException) { await Handle.DisposeAsync(); }
        catch (JSDisconnectedException) { }
    }
}
public sealed class ReactiveModel : ReactiveResource
{
    private ReactiveModel(BrowserModule module, IJSObjectReference handle) : base(module, handle) { }
    public static async ValueTask<ReactiveModel> CreateAsync(BrowserModule module, object? values = null) => new(module, await module.CreateAsync("ReactiveObject", [BrowserValue.Literal(values ?? new { })]));
    public ValueTask<T> GetAsync<T>(string property) => Module.CallJsonAsync<T>(Handle, "GetValue", [property]);
    public ValueTask SetAsync<T>(string property, T value) => Module.CallVoidAsync(Handle, "SetValue", [property, BrowserValue.Literal(value)]);
    public ValueTask SetManyAsync(object values) => Module.InvokeVoidAsync("SetReactiveValues", [Handle, BrowserValue.Literal(values)]);
    public async ValueTask<BrowserSubscription> ObserveAsync<T>(string path, Func<T, Task> next, Func<JsonElement, Task>? error = null)
    {
        await using var observable = await Module.InvokeAsync<IJSObjectReference>("ObserveReactiveValue", [Handle, path]);
        return await Module.SubscribeJsonAsync<JsonElement>(observable, "", notification => Dispatch(notification, next, error));
    }
    public async ValueTask<BrowserSubscription> ObserveChangesAsync(Func<JsonElement, Task> next)
    {
        await using var observable = await Module.InvokeAsync<IJSObjectReference>("ObserveReactiveChanges", [Handle]);
        return await Module.SubscribeJsonAsync<JsonElement>(observable, "", next);
    }
}
public sealed class BrowserCommand<TInput, TOutput> : ReactiveResource
{
    private BrowserCommand(BrowserModule module, IJSObjectReference handle) : base(module, handle) { }
    public static async ValueTask<BrowserCommand<TInput, TOutput>> CreateAsync(BrowserModule module, object execute, bool asynchronous = true, bool enabled = true, bool passSignal = false) => new(module, await module.CreateAsync("BlazorReactiveCommand", [execute, new { asynchronous, enabled, passSignal }]));
    public ValueTask<bool> GetCanExecuteAsync() => Module.GetAsync<bool>(Handle, "CanExecuteValue");
    public ValueTask<bool> GetIsExecutingAsync() => Module.GetAsync<bool>(Handle, "IsExecutingValue");
    public ValueTask SetEnabledAsync(bool enabled) => Module.CallVoidAsync(Handle, "SetEnabled", [enabled]);
    public ValueTask CancelAsync() => Module.CallVoidAsync(Handle, "Cancel");
    public async ValueTask<TOutput> ExecuteAsync(TInput input, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try { return await Module.CallJsonAsync<TOutput>(Handle, "ExecuteAsync", [BrowserValue.Literal(input)], cancellationToken); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            try { await CancelAsync(); } catch (JSDisconnectedException) { } catch (ObjectDisposedException) { }
            throw;
        }
    }
    public ValueTask<BrowserSubscription> ObserveCanExecuteAsync(Func<bool, Task> next) => Module.SubscribeJsonAsync<JsonElement>(Handle, "CanExecute", value => value.ValueKind is JsonValueKind.True or JsonValueKind.False ? next(value.GetBoolean()) : Task.CompletedTask);
    public ValueTask<BrowserSubscription> ObserveExecutingAsync(Func<bool, Task> next) => Module.SubscribeJsonAsync<JsonElement>(Handle, "IsExecuting", value => value.ValueKind is JsonValueKind.True or JsonValueKind.False ? next(value.GetBoolean()) : Task.CompletedTask);
    public ValueTask<BrowserSubscription> ObserveResultsAsync(Func<TOutput, Task> next, Func<JsonElement, Task>? error = null) => Module.SubscribeJsonAsync<JsonElement>(Handle, "Results", notification => Dispatch(notification, next, error));
    public ValueTask<BrowserSubscription> ObserveErrorsAsync(Func<JsonElement, Task> next) => Module.SubscribeJsonAsync<JsonElement>(Handle, "ThrownExceptions", next);
}
